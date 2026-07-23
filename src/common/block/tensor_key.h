
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

/*
 * Project: DingoFS
 * Created Date: 2026-05-21
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_COMMON_BLOCK_TENSOR_KEY_H_
#define DINGOFS_SRC_COMMON_BLOCK_TENSOR_KEY_H_

#include <fmt/format.h>
#include <glog/logging.h>

#include <cstdint>
#include <string>
#include <string_view>
namespace dingofs {

struct TensorKey {
  // Top-level directory under cache/stage roots where StoreKey is rooted.
  // Single source of truth — loader uses this to know where tensors live.
  static constexpr std::string_view kStoreDir = "tensor";

  TensorKey() = default;

  TensorKey(const std::string& model_name, uint32_t world_size,
            uint32_t worker_id, const std::string& chunk_hash,
            const std::string& dtype)
      : model_name(model_name),
        world_size(world_size),
        worker_id(worker_id),
        chunk_hash(chunk_hash),
        dtype(dtype) {}

  std::string Id() const { return chunk_hash; }

  std::string Filename() const {
    return fmt::format("{}@{}@{}@{}@{}", SafePart(model_name), world_size,
                       worker_id, SafePart(chunk_hash), SafePart(dtype));
  }

  std::string StoreKey() const {
    CHECK_GT(chunk_hash.size(), 4);
    return fmt::format("{}/{}/{}/{}", kStoreDir, chunk_hash.substr(0, 2),
                       chunk_hash.substr(0, 4), Filename());
  }

  uint64_t StoreSize() const { return 0; }

  bool operator==(const TensorKey& o) const {
    return world_size == o.world_size && worker_id == o.worker_id &&
           chunk_hash == o.chunk_hash && model_name == o.model_name &&
           dtype == o.dtype;
  }

  std::string model_name;
  uint32_t world_size{0};
  uint32_t worker_id{0};
  std::string chunk_hash;  // hex digest produced by LMCache
  std::string dtype;       // e.g. "float16", "bfloat16"

 private:
  static std::string SafePart(const std::string& s) {
    std::string out = s;
    for (char& c : out) {
      if (c == '/' || c == '@') {
        c = '_';
      }
    }
    return out;
  }
};
//

}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_BLOCK_TENSOR_KEY_H_
