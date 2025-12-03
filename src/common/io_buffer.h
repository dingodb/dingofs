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

#ifndef DINGOFS_SRC_COMMON_IO_BUFFER_H_
#define DINGOFS_SRC_COMMON_IO_BUFFER_H_

#include <bits/types/struct_iovec.h>
#include <butil/iobuf.h>

namespace dingofs {

// for block { put,range,cache... }:
//
// block put/cache:
//   butil::IOBuf iobuf;
//   for_each_page()
//     iobuf.append_use_data(page_addr, page_size, ...);
//   auto buffer = IOBuffer(iobuf);
//   block_cache->AsyncPut(..., Block(&buffer), ...);
//
//  block range:
//     IOBuffer buffer;
//     block_cache->AsyncRange(..., &buffer, ...);
//     auto iovs = buffer.Fetch();
//
//     struct fuse_bufvec bufvec[iovs.size()];
//     for (int i = 0; i < iovs.size(); i++) {
//       bufvec[i].mem = iovs[i].data;
//       bufvec[i].mem_size = iovs[i].size;
//     }
//     fuse_reply_data(..., bufvec, ...)
//
//   we can use direct-io if all addresses for BufferVec are aligned by
//   BLOCK_SIZE (4k).
class IOBuffer {
 public:
  IOBuffer() = default;
  explicit IOBuffer(butil::IOBuf iobuf);
  IOBuffer(const char* data, size_t size);

  butil::IOBuf& IOBuf();
  const butil::IOBuf& ConstIOBuf() const;

  void CopyTo(char* dest);
  void CopyTo(char* dest) const;
  size_t AppendTo(IOBuffer* buffer, size_t n = (size_t)-1L, size_t pos = 0);
  void Append(const IOBuffer* buffer);
  void AppendVec(const std::vector<iovec>& iovs);
  void AppendUserData(void* data, size_t size,
                      std::function<void(void*)> deleter);

  size_t Size() const;
  char* Fetch1() const;
  std::vector<iovec> Fetch() const;
  std::string Describe() const;

 private:
  butil::IOBuf iobuf_;
};

}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_IO_BUFFER_H_
