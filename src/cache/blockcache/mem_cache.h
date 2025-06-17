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

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_MEM_CACHE_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_MEM_CACHE_H_

#include <sys/types.h>

#include "cache/blockcache/cache_store.h"
#include "cache/utils/context.h"

namespace dingofs {
namespace cache {

class MemCache final : public CacheStore {
 public:
  MemCache() = default;
  ~MemCache() override = default;

  Status Start(UploadFunc) override { return Status::OK(); }
  Status Shutdown() override { return Status::OK(); }

  Status Stage(ContextSPtr, const BlockKey&, const Block&,
               StageOption) override {
    return Status::NotSupport("not support");
  }

  Status RemoveStage(ContextSPtr, const BlockKey&, RemoveStageOption) override {
    return Status::NotSupport("not support");
  }

  Status Cache(ContextSPtr, const BlockKey&, const Block&,
               CacheOption) override {
    return Status::NotSupport("not support");
  }

  Status Load(ContextSPtr, const BlockKey&, off_t, size_t, IOBuffer*,
              LoadOption) override {
    return Status::NotSupport("not support");
  }

  std::string Id() const override { return "memory_cache"; }
  bool IsRunning() const override { return false; }
  bool IsCached(const BlockKey&) const override { return false; }
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_MEM_CACHE_H_
