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

#include "cache/utils/bthread.h"

namespace dingofs {
namespace cache {

RemoteBlockCacheImpl::RemoteBlockCacheImpl(RemoteBlockCacheOption option,
                                           StorageSPtr storage)
    : running_(false),
      option_(option),
      node_manager_(std::make_unique<RemoteNodeManagerImpl>(option)),
      storage_(storage) {}

Status RemoteBlockCacheImpl::Init() {
  if (!running_.exchange(true)) {
    return node_manager_->Start();
  }
  return Status::OK();
}

Status RemoteBlockCacheImpl::Shutdown() {
  if (running_.exchange(false)) {
    node_manager_->Stop();
  }
  return Status::OK();
}

Status RemoteBlockCacheImpl::Put(const BlockKey& key, const Block& block,
                                 PutOption /*option*/) {
  auto node = node_manager_->GetNode(key.Filename());
  auto status = node->Put(key, block);
  if (!status.ok()) {
    status = storage_->Put(key.StoreKey(), block.buffer);
  }
  return status;
}

Status RemoteBlockCacheImpl::Range(const BlockKey& key, off_t offset,
                                   size_t length, IOBuffer* buffer,
                                   RangeOption option) {
  auto node = node_manager_->GetNode(key.Filename());
  auto status = node->Range(key, offset, length, buffer, option.block_size);
  if (!status.ok()) {
    status = storage_->Range(key.StoreKey(), offset, length, buffer);
  }
  return status;
}

Status RemoteBlockCacheImpl::Cache(const BlockKey& key, const Block& block,
                                   CacheOption /*option*/) {
  auto node = node_manager_->GetNode(key.Filename());
  return node->Cache(key, block);
}

Status RemoteBlockCacheImpl::Prefetch(const BlockKey& key, size_t length,
                                      PrefetchOption /*option*/) {
  auto node = node_manager_->GetNode(key.Filename());
  return node->Prefetch(key, length);
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

bool RemoteBlockCacheImpl::IsCached(const BlockKey& /*key*/) const {
  return true;
}

}  // namespace cache
}  // namespace dingofs
