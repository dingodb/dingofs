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
#include "cache/utils/access_log.h"
#include "cache/utils/bthread.h"
#include "cache/utils/offload_thread_pool.h"
#include "cache/utils/phase_timer.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

TierBlockCache::TierBlockCache(BlockCacheOption local_cache_option,
                               RemoteBlockCacheOption remote_cache_option,
                               blockaccess::BlockAccesser* block_accesser)
    : running_(false),
      storage_(std::make_shared<StorageImpl>(block_accesser)),
      local_block_cache_(
          std::make_unique<BlockCacheImpl>(local_cache_option, storage_)),
      remote_block_cache_(std::make_unique<RemoteBlockCacheImpl>(
          remote_cache_option, storage_)) {}

Status TierBlockCache::Init() {
  if (!running_.exchange(true)) {
    OffloadThreadPool::GetInstance().Init();

    auto status = storage_->Init();
    if (!status.ok()) {
      return status;
    }

    status = local_block_cache_->Init();
    if (!status.ok()) {
      return status;
    }

    return remote_block_cache_->Init();
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

Status TierBlockCache::Put(const BlockKey& key, const Block& block,
                           PutOption option) {
  Status status;
  PhaseTimer timer;
  LogGuard log([&]() {
    return absl::StrFormat("[tier] put(%s,%zu): %s%s", key.Filename(),
                           block.size, status.ToString(), timer.ToString());
  });

  if (option.writeback) {
    if (LocalEnableStage()) {
      timer.NextPhase(Phase::kLocalPut);
      status = local_block_cache_->Put(key, block, option);
    } else if (RemoteEnableStage()) {
      timer.NextPhase(Phase::kRemotePut);
      status = remote_block_cache_->Put(key, block, option);
    }
  } else {  // directly put to storage
    timer.NextPhase(Phase::kS3Put);
    status = local_block_cache_->Put(key, block, option);
  }

  return status;
}

Status TierBlockCache::Range(const BlockKey& key, off_t offset, size_t length,
                             IOBuffer* buffer, RangeOption option) {
  Status status;
  PhaseTimer timer;
  LogGuard log([&]() {
    return absl::StrFormat("[tier] range(%s,%lld,%zu): %s%s", key.Filename(),
                           offset, length, status.ToString(), timer.ToString());
  });

  // try local cache first
  if (LocalEnableCache()) {
    timer.NextPhase(Phase::kLocalRange);
    auto o = option;
    o.retrive = false;
    status = local_block_cache_->Range(key, offset, length, buffer, o);
    if (status.ok()) {
      return status;
    }
  }

  // Not found or failed for local cache

  if (RemoteEnableCache()) {  // Remote cache will always retrive storage
    timer.NextPhase(Phase::kRemoteRange);
    status = remote_block_cache_->Range(key, offset, length, buffer, option);
  } else if (option.retrive) {  // No remote cache, retrive storage
    timer.NextPhase(Phase::kS3Range);
    status = storage_->Range(key.StoreKey(), offset, length, buffer);
  } else {
    status = Status::NotFound("no cache store available for block cache");
  }

  return status;
}

Status TierBlockCache::Cache(const BlockKey& key, const Block& block,
                             CacheOption option) {
  Status status;
  PhaseTimer timer;
  LogGuard log([&]() {
    return absl::StrFormat("[tier] cache(%s,%zu): %s", key.Filename(),
                           block.size, status.ToString());
  });

  if (LocalEnableCache()) {
    status = local_block_cache_->Cache(key, block, option);
  } else if (RemoteEnableCache()) {
    status = remote_block_cache_->Cache(key, block, option);
  } else {
    status = Status::NotFound("no cache store available for block cache");
  }
  return status;
}

Status TierBlockCache::Prefetch(const BlockKey& key, size_t length,
                                PrefetchOption option) {
  Status status;
  PhaseTimer timer;
  LogGuard log([&]() {
    return absl::StrFormat("[local] refetch(%s,%zu): %s%s", key.Filename(),
                           length, status.ToString(), timer.ToString());
  });

  if (LocalEnableCache()) {
    status = local_block_cache_->Prefetch(key, length, option);
  } else if (RemoteEnableCache()) {
    status = remote_block_cache_->Prefetch(key, length, option);
  } else {
    status = Status::NotFound("no cache store available for block cache");
  }
  return status;
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
