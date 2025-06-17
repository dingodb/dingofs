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

#include "cache/config/tiercache.h"
#include "cache/remotecache/remote_node_group.h"
#include "cache/utils/bthread.h"
#include "cache/utils/context.h"

namespace dingofs {
namespace cache {

DEFINE_string(cache_group, "",
              "Cache group name to use, empty means not use cache group");

RemoteBlockCacheImpl::RemoteBlockCacheImpl(RemoteBlockCacheOption option,
                                           StorageSPtr storage)
    : running_(false),
      option_(option),
      storage_(storage),
      joiner_(std::make_unique<BthreadJoiner>()) {
  if (HasCacheStore()) {
    remote_node_ = std::make_unique<RemoteNodeGroup>(option);
  } else {
    remote_node_ = std::make_unique<NoneRemoteNode>();
  }
}

Status RemoteBlockCacheImpl::Start() {
  if (!running_.exchange(true)) {
    return remote_node_->Start();
  }
  return Status::OK();
}

Status RemoteBlockCacheImpl::Shutdown() {
  if (running_.exchange(false)) {
    return remote_node_->Shutdown();
  }
  return Status::OK();
}

Status RemoteBlockCacheImpl::Put(ContextSPtr ctx, const BlockKey& key,
                                 const Block& block, PutOption option) {
  Status status;

  if (!option.writeback) {
    status = storage_->Put(ctx, key, block);
    return status;
  }

  status = remote_node_->Put(ctx, key, block);
  if (!status.ok()) {
    status = storage_->Put(ctx, key, block);
  }

  return status;
}

Status RemoteBlockCacheImpl::Range(ContextSPtr ctx, const BlockKey& key,
                                   off_t offset, size_t length,
                                   IOBuffer* buffer, RangeOption option) {
  Status status;
  ;

  status =
      remote_node_->Range(ctx, key, offset, length, buffer, option.block_size);
  if (!status.ok() && option.retrive) {
    status = storage_->Range(ctx, key, offset, length, buffer);
  }

  return status;
}

Status RemoteBlockCacheImpl::Cache(ContextSPtr ctx, const BlockKey& key,
                                   const Block& block, CacheOption /*option*/) {
  Status status;

  status = remote_node_->Cache(ctx, key, block);
  return status;
}

Status RemoteBlockCacheImpl::Prefetch(ContextSPtr ctx, const BlockKey& key,
                                      size_t length,
                                      PrefetchOption /*option*/) {
  Status status;

  status = remote_node_->Prefetch(ctx, key, length);
  return status;
}

void RemoteBlockCacheImpl::AsyncPut(ContextSPtr ctx, const BlockKey& key,
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

void RemoteBlockCacheImpl::AsyncRange(ContextSPtr ctx, const BlockKey& key,
                                      off_t offset, size_t length,
                                      IOBuffer* buffer, AsyncCallback cb,
                                      RangeOption option) {
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

void RemoteBlockCacheImpl::AsyncCache(ContextSPtr ctx, const BlockKey& key,
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

void RemoteBlockCacheImpl::AsyncPrefetch(ContextSPtr ctx, const BlockKey& key,
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
