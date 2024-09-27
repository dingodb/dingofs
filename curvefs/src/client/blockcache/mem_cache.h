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
 * Created Date: 2024-08-26
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_BLOCKCACHE_NONE_CACHE_H_
#define CURVEFS_SRC_CLIENT_BLOCKCACHE_NONE_CACHE_H_

#include "curvefs/src/client/blockcache/cache_store.h"
#include "curvefs/src/client/blockcache/error.h"

namespace curvefs {
namespace client {
namespace blockcache {

using UploadFunc = CacheStore::UploadFunc;

class MemCache : public CacheStore {
 public:
  MemCache() = default;

  virtual ~MemCache() = default;

  BCACHE_ERROR Init(UploadFunc) override { return BCACHE_ERROR::OK; }

  BCACHE_ERROR Shutdown() override { return BCACHE_ERROR::OK; }

  BCACHE_ERROR Stage(const BlockKey&, const Block&, BlockContext) override {
    return BCACHE_ERROR::NOT_SUPPORTED;
  }

  BCACHE_ERROR RemoveStage(const BlockKey&) override {
    return BCACHE_ERROR::NOT_SUPPORTED;
  }

  BCACHE_ERROR Cache(const BlockKey&, const Block&) override {
    return BCACHE_ERROR::NOT_SUPPORTED;
  }

  BCACHE_ERROR Load(const BlockKey&, std::shared_ptr<BlockReader>&) override {
    return BCACHE_ERROR::NOT_SUPPORTED;
  }

  bool IsCached(const BlockKey&) override { return false; }

  std::string Id() override { return "memory_cache"; }
};

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BLOCKCACHE_NONE_CACHE_H_
