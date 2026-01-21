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
#include "cache/common/context.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

struct PutOption {
  bool writeback{false};
  BlockAttr block_attr{BlockAttr::kFromUnknown};
};

struct RangeOption {
  bool retrieve_storage{true};
  size_t block_whole_length{0};
  bool is_subrequest{false};
};

struct CacheOption {};
struct PrefetchOption {};

// async callback
using AsyncCallback = std::function<void(Status)>;

class BlockCache {
 public:
  virtual ~BlockCache() = default;

  virtual Status Start() { return Status::OK(); }
  virtual Status Shutdown() { return Status::OK(); }

  // block operations (sync)
  virtual Status Put(ContextSPtr /*ctx*/, const BlockKey& /*key*/,
                     const Block& /*block*/,
                     [[maybe_unused]] PutOption option = PutOption()) {
    return Status::NotSupport("not implemented");
  }

  virtual Status Range(ContextSPtr /*ctx*/, const BlockKey& /*key*/,
                       off_t /*offset*/, size_t /*length*/,
                       IOBuffer* /*buffer*/,
                       [[maybe_unused]] RangeOption option = RangeOption()) {
    return Status::NotSupport("not implemented");
  }

  virtual Status Cache(ContextSPtr /*ctx*/, const BlockKey& /* key*/,
                       const Block& /*block*/,
                       [[maybe_unused]] CacheOption option = CacheOption()) {
    return Status::NotSupport("not implemented");
  }

  virtual Status Prefetch(
      ContextSPtr /*ctx*/, const BlockKey& /*key*/, size_t /*length*/,
      [[maybe_unused]] PrefetchOption option = PrefetchOption()) {
    return Status::NotSupport("not implemented");
  }

  // block operations (async)
  virtual void AsyncPut(ContextSPtr /*ctx*/, const BlockKey& /*key*/,
                        const Block& /*block*/, AsyncCallback cb,
                        [[maybe_unused]] PutOption option = PutOption()) {
    if (cb) {
      cb(Status::NotSupport("not implemented"));
    }
  }

  virtual void AsyncRange(ContextSPtr /*ctx*/, const BlockKey& /*key*/,
                          off_t /*offset*/, size_t /*length*/,
                          IOBuffer* /*buffer*/, AsyncCallback cb,
                          [[maybe_unused]] RangeOption option = RangeOption()) {
    if (cb) {
      cb(Status::NotSupport("not implemented"));
    }
  }

  virtual void AsyncCache(ContextSPtr /*ctx*/, const BlockKey& /*key*/,
                          const Block& /*block*/, AsyncCallback cb,
                          [[maybe_unused]] CacheOption option = CacheOption()) {
    if (cb) {
      cb(Status::NotSupport("not implemented"));
    }
  }

  virtual void AsyncPrefetch(
      ContextSPtr /*ctx*/, const BlockKey& /*key*/, size_t /*length*/,
      AsyncCallback cb,
      [[maybe_unused]] PrefetchOption option = PrefetchOption()) {
    if (cb) {
      cb(Status::NotSupport("not implemented"));
    }
  }

  // utility
  virtual bool IsEnabled() const { return false; }
  virtual bool EnableStage() const { return false; }
  virtual bool EnableCache() const { return false; }
  virtual bool IsCached(const BlockKey& /*key*/) const { return false; }
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
