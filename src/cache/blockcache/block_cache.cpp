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

#include <glog/logging.h>

#include <cassert>
#include <memory>

#include "absl/cleanup/cleanup.h"
#include "cache/blockcache/block_cache_metric.h"
#include "cache/blockcache/block_cache_throttle.h"
#include "cache/blockcache/cache_store.h"
#include "cache/common/errno.h"
#include "cache/common/local_filesystem.h"
#include "cache/common/log.h"
#include "cache/common/phase_timer.h"

namespace dingofs {
namespace cache {
namespace blockcache {

using cache::common::LogGuard;
using cache::common::Phase;
using cache::common::PhaseTimer;

BlockCacheImpl::BlockCacheImpl(BlockCacheOptions options)
    : options_(options),
      running_(false),
      s3_(nullptr),  // FIXME(P0)
      stage_count_(std::make_shared<Countdown>()),
      throttle_(std::make_unique<BlockCacheThrottle>()) {
  if (options.cache_store == "none") {
    store_ = std::make_shared<MemCache>();
  } else {
    store_ = std::make_shared<DiskCacheGroup>(options.disks);
  }
  uploader_ = std::make_shared<BlockCacheUploader>(s3_, store_, stage_count_);
  metric_ = std::make_unique<BlockCacheMetric>(
      options, BlockCacheMetric::AuxMember(uploader_, throttle_));
}

Errno BlockCacheImpl::Init() {
  if (!running_.exchange(true)) {
    throttle_->Start();
    uploader_->Init(options_.upload_stage_workers,
                    options_.upload_stage_queue_size);
    return store_->Init([this](const BlockKey& key,
                               const std::string& stage_path,
                               BlockContext ctx) {
      uploader_->AddStageBlock(key, stage_path, ctx);
    });
  }
  return Errno::OK;
}

Errno BlockCacheImpl::Shutdown() {
  if (running_.exchange(false)) {
    uploader_->WaitAllUploaded();  // wait all stage blocks uploaded, FIXME
    uploader_->Shutdown();
    store_->Shutdown();
    throttle_->Stop();
  }
  return Errno::OK;
}

Errno BlockCacheImpl::Put(const BlockKey& key, const Block& block,
                          BlockContext ctx) {
  Errno rc;
  PhaseTimer timer;
  LogGuard log([&]() {
    return StrFormat("put(%s,%d): %s%s", key.Filename(), block.size, StrErr(rc),
                     timer.ToString());
  });

  auto wait = throttle_->Add(block.size);  // stage throttle
  if (options_.stage && !wait) {
    timer.NextPhase(Phase::STAGE_BLOCK);
    rc = store_->Stage(key, block, ctx);
    if (rc == Errno::OK) {
      return rc;
    } else if (rc == Errno::CACHE_FULL) {
      LOG_EVERY_SECOND(WARNING)
          << "Stage block " << key.Filename() << " failed: " << StrErr(rc);
    } else if (rc != Errno::NOT_SUPPORTED) {
      LOG(WARNING) << "Stage block " << key.Filename()
                   << " failed: " << StrErr(rc);
    }
  }

  timer.NextPhase(Phase::S3_PUT);
  rc = s3_->Put(key.StoreKey(), block.data, block.size);
  return rc;
}

Errno BlockCacheImpl::Range(const BlockKey& key, off_t offset, size_t length,
                            IOBuffer* buffer, bool retrive) {
  Errno rc;
  PhaseTimer timer;
  LogGuard log([&]() {
    return StrFormat("range(%s,%d,%d): %s%s", key.Filename(), offset, length,
                     StrErr(rc), timer.ToString());
  });

  timer.NextPhase(Phase::LOAD_BLOCK);
  rc = store_->Load(key, offset, length, buffer);
  if (rc != Errno::OK && retrive) {
    timer.NextPhase(Phase::S3_RANGE);
    // rc = s3_->Range(key.StoreKey(), offset, length, buffer); // FIXME(p0)
  }
  return rc;
}

Errno BlockCacheImpl::Cache(const BlockKey& key, const Block& block) {
  Errno rc;
  LogGuard log([&]() {
    return StrFormat("cache(%s,%d): %s", key.Filename(), block.size,
                     StrErr(rc));
  });

  rc = store_->Cache(key, block);
  return rc;
}

Errno BlockCacheImpl::Flush(uint64_t ino) {
  Errno rc;
  LogGuard log([&]() { return StrFormat("flush(%d): %s", ino, StrErr(rc)); });

  rc = stage_count_->Wait(ino);
  return rc;
}

bool BlockCacheImpl::IsCached(const BlockKey& key) {
  return store_->IsCached(key);
}

StoreType BlockCacheImpl::GetStoreType() {
  if (options_.cache_store == "none") {
    return StoreType::NONE;
  }
  return StoreType::DISK;
}

}  // namespace blockcache
}  // namespace cache
}  // namespace dingofs
