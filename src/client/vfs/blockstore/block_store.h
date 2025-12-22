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

#ifndef DINGOFS_CLIENT_BLOCK_STORE_H_
#define DINGOFS_CLIENT_BLOCK_STORE_H_

#include <cstdint>

#include "cache/blockcache/cache_store.h"
#include "common/callback.h"
#include "common/io_buffer.h"
#include "common/status.h"
#include "common/trace/context.h"

namespace dingofs {
namespace client {
namespace vfs {

using BlockKey = ::dingofs::cache::BlockKey;

struct RangeReq {
  BlockKey block;
  size_t block_size{0};
  int64_t offset{0};
  int64_t length{0};
  IOBuffer* data{nullptr};
};

struct PutReq {
  BlockKey block;
  IOBuffer data;
  bool write_back{false};
};

struct PrefetchReq {
  BlockKey block;
  size_t block_size{0};
};

class BlockStore {
 public:
  virtual ~BlockStore() = default;

  virtual Status Start() = 0;
  virtual void Shutdown() = 0;

  virtual void RangeAsync(ContextSPtr ctx, RangeReq req,
                          StatusCallback callback) = 0;

  virtual void PutAsync(ContextSPtr ctx, PutReq req,
                        StatusCallback callback) = 0;

  virtual void PrefetchAsync(ContextSPtr ctx, PrefetchReq req,
                             StatusCallback callback) = 0;
  // utility
  virtual bool EnableCache() const = 0;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
#endif  // DINGOFS_CLIENT_BLOCK_STORE_H_