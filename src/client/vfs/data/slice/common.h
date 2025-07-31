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

#ifndef DINGOFS_CLIENT_VFS_DATA_SLICE_COMMON_H_
#define DINGOFS_CLIENT_VFS_DATA_SLICE_COMMON_H_

#include <fmt/format.h>

#include <cstdint>
#include <string>

namespace dingofs {
namespace client {
namespace vfs {

struct SliceDataContext {
  const uint64_t fs_id{0};
  const uint64_t ino{0};
  const uint64_t chunk_index{0};
  const uint64_t seq{0};  // unique id for slice in current chunk, valid from 1
  const uint64_t chunk_size{0};
  const uint64_t block_size{0};
  const uint64_t page_size{0};

  explicit SliceDataContext(uint64_t p_fs_id, uint64_t p_ino,
                            uint64_t p_chunk_index, uint64_t p_seq,
                            uint64_t p_chunk_size, uint64_t p_block_size,
                            uint64_t p_page_size)
      : fs_id(p_fs_id),
        ino(p_ino),
        chunk_index(p_chunk_index),
        seq(p_seq),
        chunk_size(p_chunk_size),
        block_size(p_block_size),
        page_size(p_page_size) {}

  ~SliceDataContext() = default;

  std::string UUID() const {
    return fmt::format("{}-{}-{}", ino, chunk_index, seq);
  }
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_SLICE_DATA_H_