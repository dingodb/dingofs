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

#ifndef DINGOFS_CLIENT_VFS_DATA_BUFFER_H_
#define DINGOFS_CLIENT_VFS_DATA_BUFFER_H_

#include <cstdint>
#include <string>
#include <vector>

namespace dingofs {

class IOBuffer;

namespace client {
namespace vfs {

/* Structure for scatter/gather I/O.  */
struct IOVec {
  void* iov_base;   /* Pointer to data.  */
  uint64_t iov_len; /* Length of data.  */
};

class DataBuffer {
 public:
  DataBuffer();

  ~DataBuffer();

  IOBuffer* RawIOBuffer();

  // NOTE: DataBuffer must remain alive while using the returned iovecs
  std::vector<IOVec> GatherIOVecs() const;

  std::string Describe() const;

 private:
  // takes ownership of buffer
  IOBuffer* io_buffer_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_BUFFER_H_
