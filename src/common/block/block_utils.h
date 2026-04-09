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

#ifndef DINGOFS_COMMON_BLOCK_BLOCK_UTILS_H_
#define DINGOFS_COMMON_BLOCK_BLOCK_UTILS_H_

#include <algorithm>
#include <cstdint>
#include <vector>

#include "common/block/block_key.h"

namespace dingofs {

// Enumerate all BlockKeys for a slice's physical data.
// Each block's size is self-describing: full block_size except the last
// which may be smaller (size % block_size).
inline std::vector<BlockKey> EnumerateBlockKeys(uint64_t slice_id,
                                                uint32_t size,
                                                uint32_t block_size) {
  std::vector<BlockKey> keys;
  for (uint32_t offset = 0; offset < size; offset += block_size) {
    uint32_t index = offset / block_size;
    uint32_t actual_size = std::min(block_size, size - offset);
    keys.emplace_back(slice_id, index, actual_size);
  }

  return keys;
}

}  // namespace dingofs

#endif  // DINGOFS_COMMON_BLOCK_BLOCK_UTILS_H_
