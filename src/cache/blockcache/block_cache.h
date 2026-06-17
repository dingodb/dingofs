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

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_BLOCK_CACHE_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_BLOCK_CACHE_H_

#include <brpc/server.h>
#include <json/value.h>

#include <ostream>

#include "cache/blockcache/cache_store.h"
#include "common/block/block_handle.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

// Selects which tier of TierBlockCache to route an operation to.
//   kDefault — current tiered behavior (local first, then remote)
//   kLocal   — restrict to the local tier only; do NOT fall back to remote
//   kRemote  — restrict to the remote tier only; do NOT fall back to local
// In all cases, Range may still fall through to origin storage if
// retrieve_storage is set. Useful for pinning specific blocks to one tier
// (e.g. small blocks pinned to an in-memory local store).
enum class CacheTier : uint8_t {
  kDefault = 0,
  kLocal = 1,
  kRemote = 2,
};

struct PutOption {
  bool writeback{false};
  BlockAttr block_attr{BlockAttr::kFromUnknown};
  CacheTier tier{CacheTier::kDefault};
};

struct RangeOption {
  bool retrieve_storage{true};
  size_t block_whole_length{0};
  bool is_subrequest{false};
  CacheTier tier{CacheTier::kDefault};
};

struct CacheOption {
  CacheTier tier{CacheTier::kDefault};
};

struct PrefetchOption {
  CacheTier tier{CacheTier::kDefault};
};

// async callback
using AsyncCallback = std::function<void(Status)>;

class BlockCache {
 public:
  virtual ~BlockCache() = default;

  virtual Status Start() { return Status::OK(); }
  virtual Status Shutdown() { return Status::OK(); }

  // block operations (sync)
  virtual Status Put(BlockHandle /*handle*/, IOBuffer /*block*/,
                     PutOption /*option*/ = {}) {
    return Status::NotSupport("not implemented");
  }

  // Reads [offset, offset+length) into the caller-allocated `buffer`. The
  // distributed-cache interface (TierBlockCache / RemoteBlockCache) is strictly
  // caller-allocated: `buffer` MUST be a single contiguous writable block of
  // `length` bytes (wrap caller memory via IOBuffer::AppendUserData, or
  // AppendUserDataWithMeta with meta = RDMA rkey for a registered region); an
  // empty `buffer` is rejected. Each tier fills it IN PLACE -- zero copy when
  // the memory is RDMA-registered, otherwise at most one copy. A tier that
  // misses must leave `buffer` untouched so the next tier can fill it.
  virtual Status Range(BlockHandle /*handle*/, off_t /*offset*/,
                       size_t /*length*/, IOBuffer* /*buffer*/,
                       RangeOption /*option*/ = {}) {
    return Status::NotSupport("not implemented");
  }

  virtual Status Cache(BlockHandle /*handle*/, IOBuffer /*block*/,
                       CacheOption /*option*/ = {}) {
    return Status::NotSupport("not implemented");
  }

  virtual Status Prefetch(BlockHandle /*handle*/, size_t /*length*/,
                          PrefetchOption /*option*/ = {}) {
    return Status::NotSupport("not implemented");
  }

  // block operations (async)
  virtual void AsyncPut(BlockHandle /*handle*/, IOBuffer /*block*/,
                        AsyncCallback cb, PutOption /*option*/ = {}) {
    if (cb) {
      cb(Status::NotSupport("not implemented"));
    }
  }

  virtual void AsyncRange(BlockHandle /*handle*/, off_t /*offset*/,
                          size_t /*length*/, IOBuffer* /*buffer*/,
                          AsyncCallback cb, RangeOption /*option*/ = {}) {
    if (cb) {
      cb(Status::NotSupport("not implemented"));
    }
  }

  virtual void AsyncCache(BlockHandle /*handle*/, IOBuffer /*block*/,
                          AsyncCallback cb, CacheOption /*option*/ = {}) {
    if (cb) {
      cb(Status::NotSupport("not implemented"));
    }
  }

  virtual void AsyncPrefetch(BlockHandle /*handle*/, size_t /*length*/,
                             AsyncCallback cb, PrefetchOption /*option*/ = {}) {
    if (cb) {
      cb(Status::NotSupport("not implemented"));
    }
  }

  // utility
  virtual bool IsEnabled() const { return false; }
  virtual bool EnableStage() const { return false; }
  virtual bool EnableCache() const { return false; }
  virtual bool IsCached(const BlockHandle& /*handle*/) const { return false; }
  virtual bool Dump(Json::Value& /*value*/) const { return true; }
};

using BlockCachePtr = BlockCache*;
using BlockCacheSPtr = std::shared_ptr<BlockCache>;
using BlockCacheUPtr = std::unique_ptr<BlockCache>;

inline std::ostream& operator<<(std::ostream& os,
                                const BlockCache& block_cache) {
  os << "BlockCache{enable=" << block_cache.IsEnabled()
     << " stage=" << static_cast<int>(block_cache.EnableStage())
     << " cache=" << static_cast<int>(block_cache.EnableCache()) << "}";
  return os;
}

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_BLOCK_CACHE_H_
