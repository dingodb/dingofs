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

/*
 * Project: DingoFS
 * Created Date: 2025-05-13
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_STORAGE_BUFFER_H_
#define DINGOFS_SRC_CACHE_STORAGE_BUFFER_H_

#include <bits/types/struct_iovec.h>
#include <butil/iobuf.h>

#include <cstdint>
#include <memory>
#include <string>

namespace dingofs {
namespace cache {
namespace storage {

struct RawBuffer {
  RawBuffer(char* data, size_t size) : data(data), size(size) {}

  char* data;
  size_t size;
};

using BufferVec = std::vector<RawBuffer>;

// for block { put,range,cache }:
//
// block put/cache:
//   IOBuffer buffer;
//   for_each_page()
//     buffer.append_use_data(page_addr, page_size, ...);
//   block_cache->Put(..., buffer);
//
//  block range:
//     IOBuffer buffer;
//     block_cache->Range(..., &buffer);
//     auto buffers = buffer.Buffer();
//
//     struct fuse_bufvec bufvec[iov.size()];
//     for (int i = 0; i < iov.size(); i++) {
//       bufvec[i].mem = buffers[i].data;
//       bufvec[i].mem_size = buffers[i].size;
//     }
//     fuse_reply_data(..., bufvec, ...)
//
//   we can use direct-io if all addresses for BufferVec are aligned by
//   BLOCK_SIZE (4k).
class IOBuffer {
 public:
  IOBuffer() = default;

  explicit IOBuffer(butil::IOBuf iobuf);

  butil::IOBuf& IOBuf();

  BufferVec Buffers();

  size_t Size();

  void CopyTo(char* buffer);

 private:
  butil::IOBuf iobuf_;
};

}  // namespace storage
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_STORAGE_BUFFER_H_
