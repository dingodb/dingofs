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
 * Created Date: 2025-02-10
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_CACHE_NODE_H_
#define DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_CACHE_NODE_H_

#include <memory>

#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/cache_store.h"
#include "cache/utils/context.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

// RemoteCacheNode is the client of remote cache group node
class RemoteCacheNode {
 public:
  virtual ~RemoteCacheNode() = default;

  virtual Status Start() = 0;
  virtual Status Shutdown() = 0;

  virtual Status Put(ContextSPtr ctx, const BlockKey& key,
                     const Block& block) = 0;
  virtual Status Range(ContextSPtr ctx, const BlockKey& key, off_t offset,
                       size_t length, IOBuffer* buffer, RangeOption option) = 0;
  virtual Status Cache(ContextSPtr ctx, const BlockKey& key,
                       const Block& block) = 0;
  virtual Status Prefetch(ContextSPtr ctx, const BlockKey& key,
                          size_t length) = 0;
};

class NoneRemoteCacheNode final : public RemoteCacheNode {
 public:
  Status Start() override { return Status::OK(); }
  Status Shutdown() override { return Status::OK(); }

  Status Put(ContextSPtr, const BlockKey& /*key*/,
             const Block& /*block*/) override {
    return Status::NotSupport("Remote node is not supported");
  }

  Status Range(ContextSPtr, const BlockKey& /*key*/, off_t /*offset*/,
               size_t /*length*/, IOBuffer* /*buffer*/,
               RangeOption /*block_size*/) override {
    return Status::NotSupport("Remote node is not supported");
  }

  Status Cache(ContextSPtr, const BlockKey& /*key*/,
               const Block& /*block*/) override {
    return Status::NotSupport("Remote node is not supported");
  }

  Status Prefetch(ContextSPtr, const BlockKey& /*key*/,
                  size_t /*length*/) override {
    return Status::NotSupport("Remote node is not supported");
  }
};

using RemoteCacheNodeSPtr = std::shared_ptr<RemoteCacheNode>;
using RemoteCacheNodeUPtr = std::unique_ptr<RemoteCacheNode>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_CACHE_NODE_H_
