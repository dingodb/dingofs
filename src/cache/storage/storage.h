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
 * Created Date: 2025-05-13
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_STORAGE_STORAGE_H_
#define DINGOFS_SRC_CACHE_STORAGE_STORAGE_H_

#include "cache/blockcache/cache_store.h"
#include "cache/utils/context.h"
#include "common/io_buffer.h"

namespace dingofs {
namespace cache {

struct UploadOption {
  using AsyncCacheFunc =
      std::function<void(const BlockKey& key, const Block& block)>;

  UploadOption() : async_cache_func(nullptr) {}
  AsyncCacheFunc async_cache_func;
};

struct DownloadOption {};

class Storage {
 public:
  virtual ~Storage() = default;

  virtual Status Start() = 0;
  virtual Status Shutdown() = 0;

  virtual Status Upload(ContextSPtr ctx, const BlockKey& key,
                        const Block& block,
                        UploadOption option = UploadOption()) = 0;
  virtual Status Download(ContextSPtr ctx, const BlockKey& key, off_t offset,
                          size_t length, IOBuffer* buffer,
                          DownloadOption option = DownloadOption()) = 0;
};

using StorageUPtr = std::unique_ptr<Storage>;
using StorageSPtr = std::shared_ptr<Storage>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_STORAGE_STORAGE_H_
