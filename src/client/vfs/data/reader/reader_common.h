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

#ifndef DINGOFS_CLIENT_VFS_DATA_READER_READER_COMMON_H_
#define DINGOFS_CLIENT_VFS_DATA_READER_READER_COMMON_H_

#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>

#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {
namespace client {
namespace vfs {

struct ChunkReadReq {
  const uint32_t req_index;     // request index
  const uint64_t ino;           // ino
  const uint64_t index;         // chunk index
  const uint64_t offset;        // offset in the chunk
  const uint64_t to_read_size;  // how many bytes to read

  IOBuffer buf;  // buffer to store chunk data

  std::string ToString() const;
};

struct ReaderSharedState {
  std::mutex mtx;
  std::condition_variable cv;
  uint64_t total;
  uint64_t num_done;
  Status status;
  uint64_t read_size{0};  // total read size
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_READER_READER_COMMON_H_