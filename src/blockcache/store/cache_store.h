/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_BLOCKCACHE_STORE_CACHE_STORE_H_
#define DINGOFS_BLOCKCACHE_STORE_CACHE_STORE_H_

#include <cstdint>
#include <functional>
#include <memory>

#include "blockcache/common/block_handle.h"
#include "blockcache/core/memory/buffer_view.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/store/stats.h"
#include "common/status.h"

namespace dingofs {
namespace blockcache {

using UploadFunc = std::function<void(BlockHandle handle)>;

class CacheStore {
 public:
  virtual ~CacheStore() = default;

  virtual Future<> Start(UploadFunc uploader) = 0;
  virtual Future<> Shutdown() = 0;

  virtual Future<Status> Stage(BlockHandle handle, BufferViews block) = 0;
  virtual Future<Status> RemoveStage(BlockHandle handle) = 0;
  virtual Future<Status> Cache(BlockHandle handle, BufferViews block) = 0;
  virtual Future<Status> Load(BlockHandle handle, uint64_t offset,
                              uint32_t length, char* buffer) = 0;
  virtual Future<Status> Delete(BlockHandle handle) = 0;
  virtual Future<bool> Exists(BlockHandle handle) = 0;
  virtual Future<CacheStats> GetStats() = 0;
};

using CacheStoreUPtr = std::unique_ptr<CacheStore>;

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_STORE_CACHE_STORE_H_
