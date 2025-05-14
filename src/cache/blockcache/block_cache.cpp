/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Project: DingoFS
 * Created Date: 2024-08-05
 * Author: Jingli Chen (Wine93)
 */

#include "cache/blockcache/block_cache.h"

#include <bthread/bthread.h>
#include <glog/logging.h>

#include <cassert>
#include <memory>

#include "absl/cleanup/cleanup.h"
#include "cache/blockcache/block_cache_metric.h"
#include "cache/blockcache/block_cache_throttle.h"
#include "cache/blockcache/block_prefetcher.h"
#include "cache/blockcache/cache_store.h"
#include "cache/blockcache/disk_cache_group.h"
#include "cache/blockcache/mem_cache.h"
#include "cache/common/common.h"
#include "cache/storage/buffer.h"
#include "cache/storage/storage.h"
#include "cache/utils/access_log.h"
#include "cache/utils/phase_timer.h"
#include "utils/dingo_define.h"

namespace dingofs {
namespace cache {
namespace blockcache {

using dingofs::cache::utils::LogGuard;
using dingofs::cache::utils::Phase;
using dingofs::cache::utils::PhaseTimer;

BlockCacheImpl::BlockCacheImpl(BlockCacheOption option,
                               dataaccess::DataAccesserPtr data_accesser)
    : option_(option),
      storage_(std::make_shared<storage::StorageImpl>(data_accesser)),
      running_(false),
      stage_count_(std::make_shared<Countdown>()),
      throttle_(std::make_unique<BlockCacheThrottle>()) {
  if (option.cache_store() == "none") {
    store_ = std::make_shared<MemCache>();
  } else {
    store_ = std::make_shared<DiskCacheGroup>(option.disk_cache_options());
  }

  uploader_ =
      std::make_shared<BlockCacheUploader>(data_accesser, store_, stage_count_);
  prefetcher_ = std::make_unique<BlockPrefetcherImpl>();
  metric_ = std::make_unique<BlockCacheMetric>(
      option, BlockCacheMetric::AuxMember(uploader_, throttle_));
}

Status BlockCacheImpl::Init() {
  if (!running_.exchange(true)) {
    throttle_->Start();
    uploader_->Init(option_.upload_stage_workers(),
                    option_.upload_stage_queue_size());
    auto status =
        store_->Init([this](const BlockKey& key, const std::string& stage_path,
                            BlockContext ctx) {
          uploader_->AddStageBlock(key, stage_path, ctx);
        });
    if (!status.ok()) {
      return status;
    }

    status = prefetcher_->Init(option_.prefetch_workers(),
                               option_.prefetch_queue_size(),
                               [this](const BlockKey& key, size_t length) {
                                 return DoPrefetch(key, length);
                               });
    if (!status.ok()) {
      return status;
    }
  }
  return Status::OK();
}

Status BlockCacheImpl::Shutdown() {
  if (running_.exchange(false)) {
    auto status = prefetcher_->Shutdown();
    if (!status.ok()) {
      return status;
    }
    uploader_->WaitAllUploaded();  // wait all stage blocks uploaded
    uploader_->Shutdown();
    store_->Shutdown();
    throttle_->Stop();
  }
  return Status::OK();
}

Status BlockCacheImpl::Put(PutOption option, const BlockKey& key,
                           const Block& block) {
  Status status;
  PhaseTimer timer;
  LogGuard log([&]() {
    return StrFormat("put(%s,%d): %s%s", key.Filename(), block.size,
                     status.ToString(), timer.ToString());
  });

  if (!option.writeback) {
    timer.NextPhase(Phase::kS3Put);
    status = storage_->Put(key.StoreKey(), block.buffer);
    return status;
  }

  auto wait = throttle_->Add(block.size);  // stage throttle
  if (option_.stage() && !wait) {
    timer.NextPhase(Phase::kStageBlock);
    status = store_->Stage(CacheStore::StageOption(option.ctx), key, block);
    if (status.ok()) {
      return status;
    } else if (status.IsCacheFull()) {
      LOG_EVERY_SECOND(WARNING) << "Stage block " << key.Filename()
                                << " failed: " << status.ToString();
    } else if (!status.IsNotSupport()) {
      LOG(WARNING) << "Stage block " << key.Filename()
                   << " failed: " << status.ToString();
    }
  }

  timer.NextPhase(Phase::kS3Put);
  status = storage_->Put(key.StoreKey(), block.buffer);
  return status;
}

Status BlockCacheImpl::Range(RangeOption option, const BlockKey& key,
                             off_t offset, size_t length,
                             storage::IOBuffer* buffer) {
  Status status;
  PhaseTimer timer;
  LogGuard log([&]() {
    return StrFormat("range(%s,%d,%d): %s%s", key.Filename(), offset, length,
                     status.ToString(), timer.ToString());
  });

  timer.NextPhase(Phase::kLoadBlock);
  status = store_->Load(CacheStore::LoadOption(), key, offset, length, buffer);
  if (!status.ok() && option.retive) {
    timer.NextPhase(Phase::kS3Range);
    status = storage_->Get(key.StoreKey(), offset, length, buffer);
  }
  return status;
}

Status BlockCacheImpl::Cache(CacheOption /*option*/, const BlockKey& key,
                             const Block& block) {
  Status status;
  LogGuard log([&]() {
    return StrFormat("cache(%s,%d): %s", key.Filename(), block.size,
                     status.ToString());
  });

  status = store_->Cache(CacheStore::CacheOption(), key, block);
  return status;
}

Status BlockCacheImpl::Flush(FlushOption /*option*/, uint64_t ino) {
  Status status;
  LogGuard log(
      [&]() { return StrFormat("flush(%d): %s", ino, status.ToString()); });

  status = stage_count_->Wait(ino);
  return status;
}

void BlockCacheImpl::RunInBthread(std::function<void(BlockCache*)> func) {
  bthread_t tid;
  const bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
  auto arg = FuncArg(this, func);
  int rc = bthread_start_background(
      &tid, &attr,
      [](void* arg) -> void* {
        FuncArg* raw_arg = reinterpret_cast<FuncArg*>(arg);
        raw_arg->func(raw_arg->block_cache);
        return nullptr;
      },
      &arg);

  if (rc != 0) {
    LOG(ERROR) << "Start bthread for block cache task failed: rc = " << rc;
    func(this);
  }
}

void BlockCacheImpl::SubmitPrefetch(const BlockKey& key, size_t length) {
  prefetcher_->Submit(key, length);
}

Status BlockCacheImpl::DoPrefetch(const BlockKey& key, size_t length) {
  Status status;
  PhaseTimer timer;
  LogGuard log([&]() {
    return StrFormat("prefetch(%s,%d): %s%s", key.Filename(), length,
                     status.ToString(), timer.ToString());
  });

  if (IsCached(key)) {
    return Status::OK();
  }

  timer.NextPhase(Phase::kS3Range);
  storage::IOBuffer buffer;
  status = storage_->Get(key.StoreKey(), 0, length, &buffer);
  if (status.ok()) {
    timer.NextPhase(Phase::kCacheBlock);
    Block block(&buffer);
    status = store_->Cache(CacheStore::CacheOption(), key, block);
  }
  return status;
}

bool BlockCacheImpl::IsCached(const BlockKey& key) {
  return store_->IsCached(key);
}

StoreType BlockCacheImpl::GetStoreType() {
  if (option_.cache_store() == "none") {
    return StoreType::kNone;
  }
  return StoreType::kDisk;
}

}  // namespace blockcache
}  // namespace cache
}  // namespace dingofs
