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

#ifndef DINGOFS_COMMON_BLOCK_CACHE_KEY_H_
#define DINGOFS_COMMON_BLOCK_CACHE_KEY_H_

#include <memory>
#include <string>

namespace dingofs {

class CacheKey;
using CacheKeySPtr = std::shared_ptr<CacheKey>;

// Polymorphic identity used by the block cache. Concrete keys (BlockKey for
// file-system data blocks, TensorKey for LMCache KV cache) decide both the
// display name and the on-disk relative path.
class CacheKey {
 public:
  virtual ~CacheKey() = default;

  // Stable human-readable id; used as LRU/inflight-tracker hash key and in
  // logs. Must be unique across all keys that can coexist in the same cache.
  virtual std::string Filename() const = 0;

  // Path under the cache root, e.g. "blocks/0/4/1_0_4194304" or
  // "tensor/a3/a3f2/llama-7b@1@0@a3f2c1b4@float16".
  virtual std::string StoreKey() const = 0;

  // Make an owned copy so the cache layer can keep the key alive past the
  // caller's stack frame without forcing every caller to heap-allocate.
  virtual CacheKeySPtr Clone() const = 0;
};

}  // namespace dingofs

#endif  // DINGOFS_COMMON_BLOCK_CACHE_KEY_H_
