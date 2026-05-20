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

#ifndef DINGOFS_COMMON_BLOCK_TENSOR_KEY_H_
#define DINGOFS_COMMON_BLOCK_TENSOR_KEY_H_

#include <fmt/format.h>

#include <cstdint>
#include <memory>
#include <string>

#include "common/block/cache_key.h"

namespace dingofs {

// Identity of a KV cache chunk produced by LMCache. Mirrors the relevant
// fields of LMCache's CacheEngineKey (model_name@world_size@worker_id@
// chunk_hash@dtype) so a chunk can be located unambiguously.
struct TensorKey : public CacheKey {
  std::string model_name;
  uint32_t world_size{0};
  uint32_t worker_id{0};
  std::string chunk_hash;  // hex digest produced by LMCache
  std::string dtype;       // e.g. "float16", "bfloat16"

  TensorKey() = default;

  TensorKey(std::string _model_name, uint32_t _world_size, uint32_t _worker_id,
            std::string _chunk_hash, std::string _dtype)
      : model_name(std::move(_model_name)),
        world_size(_world_size),
        worker_id(_worker_id),
        chunk_hash(std::move(_chunk_hash)),
        dtype(std::move(_dtype)) {}

  std::string Filename() const override {
    return fmt::format("{}@{}@{}@{}@{}", model_name, world_size, worker_id,
                       chunk_hash, dtype);
  }

  // Two-level bucketing by chunk_hash prefix so a single directory does not
  // grow to millions of entries. Falls back to "0000" for short/empty hashes.
  std::string StoreKey() const override {
    const std::string& h = chunk_hash.size() >= 4 ? chunk_hash : kHashPad;
    return fmt::format("tensor/{}/{}/{}", h.substr(0, 2), h.substr(0, 4),
                       Filename());
  }

  CacheKeySPtr Clone() const override {
    return std::make_shared<TensorKey>(*this);
  }

 private:
  static inline const std::string kHashPad = "0000";
};

}  // namespace dingofs

#endif  // DINGOFS_COMMON_BLOCK_TENSOR_KEY_H_
