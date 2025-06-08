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
 * Created Date: 2025-01-13
 * Author: Jingli Chen (Wine93)
 */

#include "cache/remotecache/remote_block_cache.h"

#include <memory>

#include "cache/remotecache/remote_node.h"
#include "cache/remotecache/remote_node_group.h"
#include "cache/utils/access_log.h"
#include "cache/utils/bthread.h"
#include "cache/utils/phase_timer.h"

namespace dingofs {
namespace cache {

RemoteBlockCacheImpl::RemoteBlockCacheImpl(RemoteBlockCacheOption option,
                                           StorageSPtr storage)
    : running_(false), option_(option), storage_(storage) {
  if (HasCacheStore()) {
    remote_node_ = std::make_unique<RemoteNodeGroup>(option);
  } else {
    remote_node_ = std::make_unique<NoneRemoteNode>();
  }
}

Status RemoteBlockCacheImpl::Init() {
  if (!running_.exchange(true)) {
    return remote_node_->Init();
  }
  return Status::OK();
}

Status RemoteBlockCacheImpl::Shutdown() {
  if (running_.exchange(false)) {
    return remote_node_->Destroy();
  }
  return Status::OK();
}

Status RemoteBlockCacheImpl::Put(const BlockKey& key, const Block& block,
                                 PutOption option) {
  Status status;
  PhaseTimer timer;
  LogGuard log([&]() {
    return absl::StrFormat("[remote] put(%s,%zu): %s%s", key.Filename(),
                           block.size, status.ToString(), timer.ToString());
  });

  if (!option.writeback) {
    timer.NextPhase(Phase::kS3Put);
    status = storage_->Put(key.StoreKey(), block.buffer);
    return status;
  }

  timer.NextPhase(Phase::kRPCPut);
  status = remote_node_->Put(key, block);
  if (!status.ok()) {
    timer.NextPhase(Phase::kS3Put);
    status = storage_->Put(key.StoreKey(), block.buffer);
  }

  return status;
}

Status RemoteBlockCacheImpl::Range(const BlockKey& key, off_t offset,
                                   size_t length, IOBuffer* buffer,
                                   RangeOption option) {
  Status status;
  PhaseTimer timer;
  LogGuard log([&]() {
    return absl::StrFormat("[remote] range(%s,%lld,%zu): %s%s", key.Filename(),
                           offset, length, status.ToString(), timer.ToString());
  });

  timer.NextPhase(Phase::kRPCRange);
  status = remote_node_->Range(key, offset, length, buffer, option.block_size);
  if (!status.ok() && option.retrive) {
    timer.NextPhase(Phase::kS3Range);
    status = storage_->Range(key.StoreKey(), offset, length, buffer);
  }

  return status;
}

Status RemoteBlockCacheImpl::Cache(const BlockKey& key, const Block& block,
                                   CacheOption /*option*/) {
  Status status;
  LogGuard log([&]() {
    return absl::StrFormat("[remote] cache(%s,%zu): %s", key.Filename(),
                           block.size, status.ToString());
  });

  status = remote_node_->Cache(key, block);
  return status;
}

Status RemoteBlockCacheImpl::Prefetch(const BlockKey& key, size_t length,
                                      PrefetchOption /*option*/) {
  Status status;
  LogGuard log([&]() {
    return absl::StrFormat("[remote] refetch(%s,%zu): %s", key.Filename(),
                           length, status.ToString());
  });

  status = remote_node_->Prefetch(key, length);
  return status;
}

void RemoteBlockCacheImpl::AsyncPut(const BlockKey& key, const Block& block,
                                    AsyncCallback cb, PutOption option) {
  auto self = GetSelfSPtr();
  RunInBthread([self, key, block, cb, option]() {
    Status status = self->Put(key, block, option);
    if (cb) {
      cb(status);
    }
  });
}

void RemoteBlockCacheImpl::AsyncRange(const BlockKey& key, off_t offset,
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

void RemoteBlockCacheImpl::AsyncCache(const BlockKey& key, const Block& block,
                                      AsyncCallback cb, CacheOption option) {
  auto self = GetSelfSPtr();
  RunInBthread([self, option, key, block, cb]() {
    Status status = self->Cache(key, block, option);
    if (cb) {
      cb(status);
    }
  });
}

void RemoteBlockCacheImpl::AsyncPrefetch(const BlockKey& key, size_t length,
                                         AsyncCallback cb,
                                         PrefetchOption option) {
  auto self = GetSelfSPtr();
  RunInBthread([self, option, key, length, cb]() {
    Status status = self->Prefetch(key, length, option);
    if (cb) {
      cb(status);
    }
  });
}

bool RemoteBlockCacheImpl::HasCacheStore() const {
  return !option_.cache_group.empty();
}

// We gurantee that block cache of cache group node is always enable stage
// and cache.
bool RemoteBlockCacheImpl::EnableStage() const { return HasCacheStore(); }
bool RemoteBlockCacheImpl::EnableCache() const { return HasCacheStore(); };
bool RemoteBlockCacheImpl::IsCached(const BlockKey& /*key*/) const {
  return HasCacheStore();
}

}  // namespace cache
}  // namespace dingofs
