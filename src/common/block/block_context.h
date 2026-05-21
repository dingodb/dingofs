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

#ifndef DINGOFS_COMMON_BLOCK_BLOCK_CONTEXT_H_
#define DINGOFS_COMMON_BLOCK_BLOCK_CONTEXT_H_

#include <cstdint>

#include "common/block/block_key.h"
#include "common/block/cache_key.h"

namespace dingofs {

// BlockContext = key identity + routing metadata.
//
// `key` is the canonical BlockKey {id, index, size}. For filesystem traffic
// it carries the real block identity. For tensor (KV-cache) traffic it is a
// synthetic placeholder filled from hash(tensor_key.Filename()) so the existing
// BlockKey-typed call sites (`storage_client->Put(block_ctx.key, ...)` and
// friends) keep compiling — those S3 paths are skipped at dispatch time when
// `tensor_key` is set.
//
// `fs_id` is routing metadata (StorageClientPool selects S3 config by fs_id).
//
// `tensor_key`, when non-null, marks this context as carrying a polymorphic
// CacheKey (currently always a TensorKey). Server-side dispatch routes through
// disk_cache only (no S3 uploader). Use `ActiveKey(ctx)` to obtain the
// effective key for filename / store-path / hash-ring computations.
struct BlockContext {
  BlockKey key;
  uint32_t fs_id{0};
  CacheKeySPtr tensor_key;

  BlockContext() = default;

  BlockContext(const BlockKey& key, uint32_t fs_id) : key(key), fs_id(fs_id) {}

  BlockContext(uint64_t id, uint32_t index, uint32_t size, uint32_t fs_id)
      : key(id, index, size), fs_id(fs_id) {}
};

// The effective key for filename / store-path / hash-ring computation.
// Returns the polymorphic overlay if present, otherwise the BlockKey.
inline const CacheKey& ActiveKey(const BlockContext& ctx) {
  if (ctx.tensor_key) {
    return *ctx.tensor_key;
  }
  return static_cast<const CacheKey&>(ctx.key);
}

inline bool IsTensorContext(const BlockContext& ctx) {
  return static_cast<bool>(ctx.tensor_key);
}

}  // namespace dingofs

#endif  // DINGOFS_COMMON_BLOCK_BLOCK_CONTEXT_H_
