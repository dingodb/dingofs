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

#ifndef DINGOFS_COMMON_BLOCK_BLOCK_KEY_H_
#define DINGOFS_COMMON_BLOCK_BLOCK_KEY_H_

#include <fmt/format.h>

#include <cstdint>
#include <string>

namespace dingofs {

struct BlockKey {
  uint64_t id{0};     // slice ID (globally unique)
  uint32_t index{0};  // block index within the slice
  uint32_t size{0};   // block size in bytes

  BlockKey(uint64_t _id, uint32_t _index, uint32_t _size)
      : id(_id), index(_index), size(_size) {}

  std::string Filename() const {
    return fmt::format("{}_{}_{}", id, index, size);
  }

  // Storage path with two-level directory bucketing by slice id.
  std::string StoreKey() const {
    return fmt::format("blocks/{}/{}/{}", id / 1000 / 1000, id / 1000,
                       Filename());
  }
};

}  // namespace dingofs

#endif  // DINGOFS_COMMON_BLOCK_BLOCK_KEY_H_