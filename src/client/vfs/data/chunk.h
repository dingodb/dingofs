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

#ifndef DINGOFS_CLIENT_VFS_DATA_CHUNK_H_
#define DINGOFS_CLIENT_VFS_DATA_CHUNK_H_

#include <cstdint>
#include <string>
namespace dingofs {
namespace client {
namespace vfs {

struct Chunk {
  const uint64_t fs_id{0};
  const uint64_t ino{0};
  const uint64_t index{0};
  const uint64_t chunk_size{0};
  const uint64_t block_size{0};
  const uint64_t chunk_start{0};  // in file offset
  const uint64_t chunk_end{0};    // in file offset

  explicit Chunk(uint64_t _fs_id, uint64_t _ino, uint64_t _index,
                 uint64_t _chunk_size, uint64_t _block_size)
      : fs_id(_fs_id),
        ino(_ino),
        index(_index),
        chunk_size(_chunk_size),
        block_size(_block_size),
        chunk_start(_index * _chunk_size),
        chunk_end(chunk_start + _chunk_size) {}

  std::string UUID() const;
  std::string ToString() const;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_CHUNK_H_