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

#include "cache/blockcache/block_cache_impl.h"

#include <memory>

#include "cache/blockcache/disk_cache_group.h"
#include "cache/blockcache/mem_cache.h"
#include "cache/storage/storage.h"
#include "cache/storage/storage_pool.h"
#include "cache/utils/access_log.h"
#include "cache/utils/bthread.h"
#include "cache/utils/phase_timer.h"
#include "common/io_buffer.h"

namespace dingofs {
namespace cache {

BlockCacheImpl::BlockCacheImpl(BlockCacheOption option, StorageSPtr storage)
    : BlockCacheImpl(option, std::make_shared<SingleStorage>(storage)) {}

BlockCacheImpl::BlockCacheImpl(BlockCacheOption option,
                               StoragePoolSPtr storage_pool)
    : running_(false), option_(option), storage_pool_(storage_pool) {
  if (HasCacheStore()) {
    store_ = std::make_shared<DiskCacheGroup>(option.disk_cache_options);
  } else {
    store_ = std::make_shared<MemCache>();
  }
  uploader_ = std::make_shared<BlockCacheUploader>(storage_pool_, store_);
  prefetch_throttle_ =
      std::make_shared<InflightThrottle>(FLAGS_prefetch_max_inflights);
}

Status BlockCacheImpl::Init() {
  if (!running_.exchange(true)) {
    uploader_->Init();
    return store_->Init(
        [this](const BlockKey& key, size_t length, BlockContext ctx) {
          uploader_->AddStageBlock(StageBlock(key, length, ctx));
        });
  }
  return Status::OK();
}

Status BlockCacheImpl::Shutdown() {
  if (running_.exchange(false)) {
    uploader_->WaitAllUploaded();  // wait all stage blocks uploaded
    uploader_->Shutdown();
    store_->Shutdown();
  }
  return Status::OK();
}

Status BlockCacheImpl::Put(const BlockKey& key, const Block& block,
                           PutOption option) {
  Status status;
  PhaseTimer timer;
  LogGuard log([&]() {
    return absl::StrFormat("[local] put(%s,%zu): %s%s", key.Filename(),
                           block.size, status.ToString(), timer.ToString());
  });

  if (!option.writeback) {
    timer.NextPhase(Phase::kS3Put);
    status = StoragePut(key, block.buffer);
    return status;
  }

  // writeback: stage block
  timer.NextPhase(Phase::kStageBlock);
  status = store_->Stage(key, block, CacheStore::StageOption(option.ctx));
  if (status.ok()) {
    return status;
  } else if (status.IsCacheFull()) {
    LOG_EVERY_SECOND(WARNING) << "Stage block (key=" << key.Filename()
                              << ") failed: " << status.ToString();
  } else if (!status.IsNotSupport()) {
    LOG(WARNING) << "Stage block (key=" << key.Filename()
                 << ") failed: " << status.ToString();
  }

  // stage block failed, try to upload it
  timer.NextPhase(Phase::kS3Put);
  status = StoragePut(key, block.buffer);
  return status;
}

Status BlockCacheImpl::Range(const BlockKey& key, off_t offset, size_t length,
                             IOBuffer* buffer, RangeOption option) {
  Status status;
  PhaseTimer timer;
  LogGuard log([&]() {
    return absl::StrFormat("[local] range(%s,%lld,%zu): %s%s", key.Filename(),
                           offset, length, status.ToString(), timer.ToString());
  });

  timer.NextPhase(Phase::kLoadBlock);
  status = store_->Load(key, offset, length, buffer);
  if (!status.ok() && option.retrive) {
    timer.NextPhase(Phase::kS3Range);
    status = StorageRange(key, offset, length, buffer);
  }
  return status;
}

Status BlockCacheImpl::Cache(const BlockKey& key, const Block& block,
                             CacheOption /*option*/) {
  Status status;
  LogGuard log([&]() {
    return absl::StrFormat("[local] cache(%s,%zu): %s", key.Filename(),
                           block.size, status.ToString());
  });

  status = store_->Cache(key, block);
  return status;
}

Status BlockCacheImpl::Prefetch(const BlockKey& key, size_t length,
                                PrefetchOption /*option*/) {
  Status status;
  PhaseTimer timer;
  LogGuard log([&]() {
    return absl::StrFormat("[local] refetch(%s,%zu): %s%s", key.Filename(),
                           length, status.ToString(), timer.ToString());
  });

  if (IsCached(key)) {
    return Status::OK();
  }

  timer.NextPhase(Phase::kS3Range);
  IOBuffer buffer;
  status = StorageRange(key, 0, length, &buffer);
  if (status.ok()) {
    timer.NextPhase(Phase::kCacheBlock);
    status = store_->Cache(key, Block(buffer));
  }

  return status;
}

void BlockCacheImpl::AsyncPut(const BlockKey& key, const Block& block,
                              AsyncCallback cb, PutOption option) {
  auto self = GetSelfSPtr();
  RunInBthread([self, key, block, cb, option]() {
    Status status = self->Put(key, block, option);
    if (cb) {
      cb(status);
    }
  });
}

void BlockCacheImpl::AsyncRange(const BlockKey& key, off_t offset,
                                size_t length, IOBuffer* buffer,
                                AsyncCallback cb, RangeOption option) {
  auto self = GetSelfSPtr();
  RunInBthread([self, key, offset, length, buffer, cb, option]() {
    Status status = self->Range(key, offset, length, buffer, option);
    if (cb) {
      cb(status);
    }
  });
}

void BlockCacheImpl::AsyncCache(const BlockKey& key, const Block& block,
                                AsyncCallback cb, CacheOption option) {
  auto self = GetSelfSPtr();
  RunInBthread([self, option, key, block, cb]() {
    Status status = self->Cache(key, block, option);
    if (cb) {
      cb(status);
    }
  });
}

void BlockCacheImpl::AsyncPrefetch(const BlockKey& key, size_t length,
                                   AsyncCallback cb, PrefetchOption option) {
  InflightThrottleGuard guard(prefetch_throttle_,
                              1);  // TODO: acts on sync op
  auto self = GetSelfSPtr();
  RunInBthread([self, option, key, length, cb]() {
    Status status = self->Prefetch(key, length, option);
    if (cb) {
      cb(status);
    }
  });
}

Status BlockCacheImpl::StoragePut(const BlockKey& key, const IOBuffer& buffer) {
  StorageSPtr storage;
  auto status = storage_pool_->GetStorage(key.fs_id, storage);
  if (status.ok()) {
    status = storage->Put(key.StoreKey(), buffer);
  }
  return status;
}

Status BlockCacheImpl::StorageRange(const BlockKey& key, off_t offset,
                                    size_t length, IOBuffer* buffer) {
  StorageSPtr storage;
  auto status = storage_pool_->GetStorage(key.fs_id, storage);
  if (status.ok()) {
    status = storage->Range(key.StoreKey(), offset, length, buffer);
  }
  return status;
}

bool BlockCacheImpl::HasCacheStore() const {
  return option_.cache_store != "none";
}

bool BlockCacheImpl::EnableStage() const {
  return HasCacheStore() && option_.enable_stage;
}

bool BlockCacheImpl::EnableCache() const {
  return HasCacheStore() && option_.enable_cache;
}

bool BlockCacheImpl::IsCached(const BlockKey& key) const {
  return store_->IsCached(key);
}

}  // namespace cache
}  // namespace dingofs
