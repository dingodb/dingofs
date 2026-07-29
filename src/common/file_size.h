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

#ifndef DINGOFS_SRC_COMMON_FILE_SIZE_H_
#define DINGOFS_SRC_COMMON_FILE_SIZE_H_

#include <cstdint>
#include <limits>

namespace dingofs {

// Chunk indexes are uint32. Reserve the upper half of the index space.
constexpr uint64_t kMaxFileChunkCount = uint64_t{1} << 31;

inline bool TryGetMaxFileSize(uint64_t chunk_size, uint64_t* max_file_size) {
  if (max_file_size == nullptr || chunk_size == 0 ||
      chunk_size > std::numeric_limits<uint64_t>::max() / kMaxFileChunkCount) {
    return false;
  }

  *max_file_size = chunk_size * kMaxFileChunkCount;
  return true;
}

inline bool IsValidFileSize(uint64_t size, uint64_t max_file_size) {
  return size <= max_file_size;
}

inline bool IsValidFileRange(uint64_t offset, uint64_t length,
                             uint64_t max_file_size) {
  if (length == 0) return true;
  return offset < max_file_size && length <= max_file_size - offset;
}

}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_FILE_SIZE_H_
