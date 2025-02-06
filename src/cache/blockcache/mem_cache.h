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

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_NONE_CACHE_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_NONE_CACHE_H_

#include "cache/blockcache/cache_store.h"
#include "cache/common/errno.h"

namespace dingofs {
namespace cache {
namespace blockcache {

using UploadFunc = CacheStore::UploadFunc;

class MemCache : public CacheStore {
 public:
  MemCache() = default;

  ~MemCache() override = default;

  Errno Init(UploadFunc) override { return Errno::OK; }

  Errno Shutdown() override { return Errno::OK; }

  Errno Stage(const BlockKey&, const Block&, BlockContext) override {
    return Errno::NOT_SUPPORTED;
  }

  Errno RemoveStage(const BlockKey&, BlockContext) override {
    return Errno::NOT_SUPPORTED;
  }

  Errno Cache(const BlockKey&, const Block&) override {
    return Errno::NOT_SUPPORTED;
  }

  Errno Load(const BlockKey&, std::shared_ptr<BlockReader>&) override {
    return Errno::NOT_SUPPORTED;
  }

  Errno Load(const BlockKey&, uint64_t /*offset*/, uint64_t /*length*/,
             IOBuffer* /*buffer*/) override {
    return Errno::NOT_SUPPORTED;
  }

  bool IsCached(const BlockKey&) override { return false; }

  std::string Id() override { return "memory_cache"; }
};

}  // namespace blockcache
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_NONE_CACHE_H_
