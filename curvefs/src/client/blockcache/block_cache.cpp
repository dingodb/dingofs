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

#include "curvefs/src/client/blockcache/block_cache.h"

#include <glog/logging.h>

#include <cassert>
#include <memory>

#include "absl/cleanup/cleanup.h"
#include "curvefs/src/client/blockcache/block_cache_metric.h"
#include "curvefs/src/client/blockcache/disk_cache_group.h"
#include "curvefs/src/client/blockcache/error.h"
#include "curvefs/src/client/blockcache/local_filesystem.h"
#include "curvefs/src/client/blockcache/log.h"
#include "curvefs/src/client/blockcache/mem_cache.h"
#include "curvefs/src/client/blockcache/phase_timer.h"

namespace curvefs {
namespace client {
namespace blockcache {

BlockCacheImpl::BlockCacheImpl(BlockCacheOption option)
    : option_(option),
      running_(false),
      s3_(S3ClientImpl::GetInstance()),
      stage_count_(std::make_shared<Countdown>()) {
  if (option.cache_store == "none") {
    store_ = std::make_shared<MemCache>();
  } else {
    store_ = std::make_shared<DiskCacheGroup>(option.disk_cache_options);
  }
  uploader_ = std::make_shared<BlockCacheUploader>(store_, s3_, stage_count_);

  auto aux_member = BlockCacheMetric::AuxMember{.uploader = uploader_};
  metric_ = std::make_unique<BlockCacheMetric>(option, aux_member);
}

BCACHE_ERROR BlockCacheImpl::Init() {
  if (!running_.exchange(true)) {
    uploader_->Init(option_.upload_stage_workers,
                    option_.upload_stage_queue_size);
    return store_->Init([this](const BlockKey& key,
                               const std::string& stage_path,
                               bool from_reload) {
      uploader_->AddStageBlock(key, stage_path, from_reload);
    });
  }
  return BCACHE_ERROR::OK;
}

BCACHE_ERROR BlockCacheImpl::Shutdown() {
  if (running_.exchange(false)) {
    WaitAllStageUploaded();
    store_->Shutdown();
  }
  return BCACHE_ERROR::OK;
}

BCACHE_ERROR BlockCacheImpl::Put(const BlockKey& key, const Block& block) {
  BCACHE_ERROR rc;
  PhaseTimer timer;
  LogGuard log([&]() {
    return StrFormat("put(%s,%d): %s%s", key.Filename(), block.size, StrErr(rc),
                     timer.ToString());
  });

  if (option_.stage) {
    timer.NextPhase(Phase::STAGE_BLOCK);
    // TODO: Block should upload to s3 directly if disk reach bandwith
    // throttle for perfomance.
    rc = store_->Stage(key, block);
    if (rc == BCACHE_ERROR::OK) {
      return rc;
    } else if (rc == BCACHE_ERROR::CACHE_FULL) {
      LOG_EVERY_SECOND(WARNING)
          << "Stage block " << key.Filename() << " failed: " << StrErr(rc);
    } else if (rc != BCACHE_ERROR::NOT_SUPPORTED) {
      LOG(WARNING) << "Stage block " << key.Filename()
                   << " failed: " << StrErr(rc);
    }
  }

  // TODO: Cache the block which put to storage directly
  timer.NextPhase(Phase::S3_PUT);
  rc = s3_->Put(key.StoreKey(), block.data, block.size);
  return rc;
}

BCACHE_ERROR BlockCacheImpl::Range(const BlockKey& key, off_t offset,
                                   size_t length, char* buffer, bool retrive) {
  BCACHE_ERROR rc;
  PhaseTimer timer;
  LogGuard log([&]() {
    return StrFormat("range(%s,%d,%d): %s%s", key.Filename(), offset, length,
                     StrErr(rc), timer.ToString());
  });

  timer.NextPhase(Phase::LOAD_BLOCK);
  std::shared_ptr<BlockReader> reader;
  rc = store_->Load(key, reader);
  if (rc == BCACHE_ERROR::OK) {
    timer.NextPhase(Phase::READ_BLOCK);
    auto defer = ::absl::MakeCleanup([reader]() { reader->Close(); });
    rc = reader->ReadAt(offset, length, buffer);
    if (rc == BCACHE_ERROR::OK) {
      return rc;
    }
  }

  timer.NextPhase(Phase::S3_RANGE);
  if (retrive) {
    rc = s3_->Range(key.StoreKey(), offset, length, buffer);
  }
  return rc;
}

BCACHE_ERROR BlockCacheImpl::Cache(const BlockKey& key, const Block& block) {
  BCACHE_ERROR rc;
  LogGuard log([&]() {
    return StrFormat("cache(%s,%d): %s", key.Filename(), block.size,
                     StrErr(rc));
  });

  rc = store_->Cache(key, block);
  return rc;
}

BCACHE_ERROR BlockCacheImpl::Flush(uint64_t ino) {
  BCACHE_ERROR rc = BCACHE_ERROR::OK;
  LogGuard log([&]() { return StrFormat("flush(%d): %s", ino, StrErr(rc)); });

  stage_count_->Wait(ino);
  return rc;
}

bool BlockCacheImpl::IsCached(const BlockKey& key) {
  return store_->IsCached(key);
}

StoreType BlockCacheImpl::GetStoreType() {
  if (option_.cache_store == "none") {
    return StoreType::NONE;
  }
  return StoreType::DISK;
}

void BlockCacheImpl::WaitAllStageUploaded() {
  while (uploader_->GetPendingSize() != 0) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs
