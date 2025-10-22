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

#include "common/io_buffer.h"

#include <bits/types/struct_iovec.h>
#include <butil/iobuf.h>
#include <glog/logging.h>

#include <sstream>

namespace dingofs {

IOBuffer::IOBuffer(butil::IOBuf iobuf) : iobuf_(iobuf) {}

IOBuffer::IOBuffer(const char* data, size_t size) { iobuf_.append(data, size); }

butil::IOBuf& IOBuffer::IOBuf() { return iobuf_; }

const butil::IOBuf& IOBuffer::ConstIOBuf() const { return iobuf_; }

void IOBuffer::CopyTo(char* dest) { iobuf_.copy_to(dest); }

void IOBuffer::CopyTo(char* dest) const { iobuf_.copy_to(dest); }

void IOBuffer::AppendTo(IOBuffer* buffer, size_t n, size_t pos) {
  iobuf_.append_to(&buffer->IOBuf(), n, pos);
}

void IOBuffer::Append(const IOBuffer* buffer) { iobuf_.append(buffer->iobuf_); }

void IOBuffer::AppendVec(const std::vector<iovec>& iovs) {
  iobuf_.appendv((const const_iovec*)iovs.data(), iovs.size());
}

void IOBuffer::AppendUserData(void* data, size_t size,
                              std::function<void(void*)> deleter) {
  iobuf_.append_user_data(data, size, deleter);
}

size_t IOBuffer::Size() const { return iobuf_.length(); }

char* IOBuffer::Fetch1() const {
  CHECK_EQ(iobuf_.backing_block_num(), 1);
  return (char*)iobuf_.fetch1();
}

std::vector<iovec> IOBuffer::Fetch() const {
  std::vector<iovec> iovecs;
  for (int i = 0; i < iobuf_.backing_block_num(); i++) {
    const auto& string_piece = iobuf_.backing_block(i);

    char* data = (char*)string_piece.data();
    size_t size = string_piece.length();
    iovecs.emplace_back(iovec{data, size});
  }
  return iovecs;
}

std::string IOBuffer::Describe() const {
  const auto& iovecs = Fetch();
  if (iovecs.empty()) {
    return "IOBuffer[]";
  }

  std::ostringstream oss;
  oss << "IOBuffer[";
  auto vec_count = iovecs.size();
  for (int i = 0; i < vec_count; ++i) {
    oss << "(" << i << ", " << iovecs[i].iov_base << ", " << iovecs[i].iov_len
        << ")";
    if (i < vec_count - 1) {
      oss << ",";
    }
  }
  oss << ", (size: " << iobuf_.length() << ")]";

  return oss.str();
}

}  // namespace dingofs
