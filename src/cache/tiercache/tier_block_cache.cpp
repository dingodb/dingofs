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

#include "blockaccess/block_accesser.h"
#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/block_cache_impl.h"
#include "cache/remotecache/remote_block_cache.h"
#include "cache/storage/storage.h"
#include "cache/storage/storage_impl.h"
#include "cache/utils/bthread.h"
#include "cache/utils/context.h"
#include "cache/utils/offload_thread_pool.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

TierBlockCache::TierBlockCache(BlockCacheOption local_cache_option,
                               RemoteBlockCacheOption remote_cache_option,
                               StorageSPtr storage)
    : running_(false),
      storage_(storage),
      local_block_cache_(
          std::make_unique<BlockCacheImpl>(local_cache_option, storage_)),
      remote_block_cache_(std::make_unique<RemoteBlockCacheImpl>(
          remote_cache_option, storage_)) {}

TierBlockCache::TierBlockCache(BlockCacheOption local_cache_option,
                               RemoteBlockCacheOption remote_cache_option,
                               blockaccess::BlockAccesser* block_accesser)
    : TierBlockCache(local_cache_option, remote_cache_option,
                     std::make_shared<StorageImpl>(block_accesser)) {}

Status TierBlockCache::Start() {
  if (!running_.exchange(true)) {
    OffloadThreadPool::GetInstance().Start();

    auto status = storage_->Start();
    if (!status.ok()) {
      return status;
    }

    status = local_block_cache_->Start();
    if (!status.ok()) {
      return status;
    }

    return remote_block_cache_->Start();
  }
  return Status::OK();
}

Status TierBlockCache::Shutdown() {
  if (running_.exchange(false)) {
    auto status = local_block_cache_->Shutdown();
    if (!status.ok()) {
      return status;
    }

    status = remote_block_cache_->Shutdown();
    if (!status.ok()) {
      return status;
    }

    return storage_->Shutdown();
  }
  return Status::OK();
}

Status TierBlockCache::Put(ContextSPtr ctx, const BlockKey& key,
                           const Block& block, PutOption option) {
  Status status;

  if (option.writeback) {
    if (LocalEnableStage()) {
      status = local_block_cache_->Put(ctx, key, block, option);
    } else if (RemoteEnableStage()) {
      status = remote_block_cache_->Put(ctx, key, block, option);
    }
  } else {  // directly put to storage

    status = local_block_cache_->Put(ctx, key, block, option);
  }

  return status;
}

Status TierBlockCache::Range(ContextSPtr ctx, const BlockKey& key, off_t offset,
                             size_t length, IOBuffer* buffer,
                             RangeOption option) {
  Status status;

  // try local cache first
  if (LocalEnableCache()) {
    auto opt = option;
    opt.retrive = false;
    status = local_block_cache_->Range(ctx, key, offset, length, buffer, opt);
    if (status.ok()) {
      return status;
    }
  }

  // Not found or failed for local cache

  if (RemoteEnableCache()) {  // Remote cache will always retrive storage

    status =
        remote_block_cache_->Range(ctx, key, offset, length, buffer, option);
  } else if (option.retrive) {  // No remote cache, retrive storage

    status = storage_->Range(ctx, key, offset, length, buffer);
  } else {
    LOG(ERROR) << "";
    status = Status::NotFound("no cache store available for block cache");
  }

  return status;
}

Status TierBlockCache::Cache(ContextSPtr ctx, const BlockKey& key,
                             const Block& block, CacheOption option) {
  Status status;

  if (LocalEnableCache()) {
    status = local_block_cache_->Cache(ctx, key, block, option);
  } else if (RemoteEnableCache()) {
    status = remote_block_cache_->Cache(ctx, key, block, option);
  } else {
    status = Status::NotFound("no cache store available for block cache");
  }
  return status;
}

Status TierBlockCache::Prefetch(ContextSPtr ctx, const BlockKey& key,
                                size_t length, PrefetchOption option) {
  Status status;

  if (LocalEnableCache()) {
    status = local_block_cache_->Prefetch(ctx, key, length, option);
  } else if (RemoteEnableCache()) {
    status = remote_block_cache_->Prefetch(ctx, key, length, option);
  } else {
    status = Status::NotFound("no cache available");
  }
  return status;
}

void TierBlockCache::AsyncPut(ContextSPtr ctx, const BlockKey& key,
                              const Block& block, AsyncCallback cb,
                              PutOption option) {
  auto* self = GetSelfPtr();
  auto tid = RunInBthread([self, ctx, key, block, cb, option]() {
    Status status = self->Put(ctx, key, block, option);
    if (cb) {
      cb(status);
    }
  });

  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }
}

void TierBlockCache::AsyncRange(ContextSPtr ctx, const BlockKey& key,
                                off_t offset, size_t length, IOBuffer* buffer,
                                AsyncCallback cb, RangeOption option) {
  auto* self = GetSelfPtr();
  auto tid =
      RunInBthread([self, ctx, key, offset, length, buffer, cb, option]() {
        Status status = self->Range(ctx, key, offset, length, buffer, option);
        if (cb) {
          cb(status);
        }
      });

  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }
}

void TierBlockCache::AsyncCache(ContextSPtr ctx, const BlockKey& key,
                                const Block& block, AsyncCallback cb,
                                CacheOption option) {
  auto* self = GetSelfPtr();
  auto tid = RunInBthread([self, ctx, key, block, cb, option]() {
    Status status = self->Cache(ctx, key, block, option);
    if (cb) {
      cb(status);
    }
  });

  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }
}

void TierBlockCache::AsyncPrefetch(ContextSPtr ctx, const BlockKey& key,
                                   size_t length, AsyncCallback cb,
                                   PrefetchOption option) {
  auto* self = GetSelfPtr();
  auto tid = RunInBthread([self, ctx, key, length, cb, option]() {
    Status status = self->Prefetch(ctx, key, length, option);
    if (cb) {
      cb(status);
    }
  });

  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }
}

bool TierBlockCache::LocalEnableStage() const {
  return local_block_cache_->EnableStage();
}

bool TierBlockCache::LocalEnableCache() const {
  return local_block_cache_->EnableCache();
}

bool TierBlockCache::RemoteEnableStage() const {
  return remote_block_cache_->EnableStage();
}

bool TierBlockCache::RemoteEnableCache() const {
  return remote_block_cache_->EnableCache();
}

bool TierBlockCache::HasCacheStore() const { return true; }
bool TierBlockCache::EnableStage() const { return true; }
bool TierBlockCache::EnableCache() const { return true; }

bool TierBlockCache::IsCached(const BlockKey& key) const {
  if (LocalEnableCache() && local_block_cache_->IsCached(key)) {
    return true;
  } else if (RemoteEnableCache() && remote_block_cache_->IsCached(key)) {
    return true;
  }
  return false;
}

}  // namespace cache
}  // namespace dingofs
