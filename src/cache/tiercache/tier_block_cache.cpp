/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2025-05-27
 * Author: Jingli Chen (Wine93)
 */

#include "cache/tiercache/tier_block_cache.h"

#include <memory>

#include "cache/blockcache/block_cache_impl.h"
#include "cache/remotecache/remote_block_cache.h"
#include "cache/storage/storage.h"
#include "cache/storage/storage_impl.h"
#include "cache/utils/bthread.h"

namespace dingofs {
namespace cache {

TierBlockCache::TierBlockCache(BlockCacheOption local_cache_option,
                               RemoteBlockCacheOption remote_cache_option,
                               StorageSPtr storage)
    : local_block_cache_(
          std::make_unique<BlockCacheImpl>(local_cache_option, storage)),
      remote_block_cache_(std::make_unique<RemoteBlockCacheImpl>(
          remote_cache_option, storage)) {}

Status TierBlockCache::Init() {
  if (!running_.exchange(true)) {
    auto status = local_block_cache_->Init();
    if (status.ok()) {
      status = remote_block_cache_->Init();
    }
    return status;
  }
  return Status::OK();
}

Status TierBlockCache::Shutdown() {
  if (running_.exchange(false)) {
    auto status = local_block_cache_->Shutdown();
    if (status.ok()) {
      status = remote_block_cache_->Shutdown();
    }
    return status;
  }
  return Status::OK();
}

Status TierBlockCache::Put(const BlockKey& key, const Block& block,
                           PutOption option) {
  Status status;
  if (option.writeback) {
    // if (local_block_cache_->EnableStage()) {
    //   status = local_block_cache_->Put(option, key, block);
    // } else if (remote_block_cache_->EnableStage()) {
    //   status = remote_block_cache_->Put(option, key, block);
    // }

    status = remote_block_cache_->Put(key, block, option);
  } else {  // directly put to storage
    status = local_block_cache_->Put(key, block, option);
  }
  return status;
}

Status TierBlockCache::Range(const BlockKey& key, off_t offset, size_t length,
                             IOBuffer* buffer, RangeOption option) {
  Status status = Status::NotFound("block not found");

  // try local block cache
  // If (local_block_cache_->EnableStage()) {
  //  status = local_block_cache_->Range(WithoutRetrive(option), key, offset,
  //                                     length, buffer);
  //}

  // then try remote block cache
  // if (status.IsNotFound() && remote_block_cache_->EnableCache()) {
  status = remote_block_cache_->Range(key, offset, length, buffer, option);
  //}

  return status;
}

Status TierBlockCache::Cache(const BlockKey& key, const Block& block,
                             CacheOption option) {
  return local_block_cache_->Cache(key, block, option);
}

Status TierBlockCache::Prefetch(const BlockKey& key, size_t length,
                                PrefetchOption option) {
  return local_block_cache_->Prefetch(key, length, option);
}

void TierBlockCache::AsyncPut(const BlockKey& key, const Block& block,
                              AsyncCallback cb, PutOption option) {
  auto self = GetSelfSPtr();
  RunInBthread([self, key, block, cb, option]() {
    Status status = self->Put(key, block, option);
    if (cb) {
      cb(status);
    }
  });
}

void TierBlockCache::AsyncRange(const BlockKey& key, off_t offset,
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

void TierBlockCache::AsyncCache(const BlockKey& key, const Block& block,
                                AsyncCallback cb, CacheOption option) {
  auto self = GetSelfSPtr();
  RunInBthread([self, key, block, cb, option]() {
    Status status = self->Cache(key, block, option);
    if (cb) {
      cb(status);
    }
  });
}

void TierBlockCache::AsyncPrefetch(const BlockKey& key, size_t length,
                                   AsyncCallback cb, PrefetchOption option) {
  auto self = GetSelfSPtr();
  RunInBthread([self, key, length, cb, option]() {
    Status status = self->Prefetch(key, length, option);
    if (cb) {
      cb(status);
    }
  });
}

bool TierBlockCache::IsCached(const BlockKey& /*key*/) const { return true; }

}  // namespace cache
}  // namespace dingofs
