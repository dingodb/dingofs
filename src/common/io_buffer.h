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

class IOBuffer {
 public:
  IOBuffer() = default;
  ~IOBuffer() = default;
  IOBuffer(const IOBuffer& buffer) = default;
  IOBuffer& operator=(const IOBuffer& buffer) = default;
  IOBuffer(IOBuffer&& buffer) noexcept : iobuf_(buffer.iobuf_.movable()) {}
  IOBuffer& operator=(IOBuffer&& buffer) noexcept {
    if (this != &buffer) {
      iobuf_ = buffer.iobuf_.movable();
    }
    return *this;
  }

  explicit IOBuffer(butil::IOBuf iobuf) : iobuf_(iobuf) {}
  explicit IOBuffer(butil::IOBuf::Movable& iobuf) : iobuf_(iobuf) {}
  IOBuffer(const char* data, size_t size) { iobuf_.append(data, size); }
  butil::IOBuf& IOBuf() { return iobuf_; }
  const butil::IOBuf& ConstIOBuf() const { return iobuf_; }

  void CopyTo(char* dest, size_t n = (size_t)-1L, size_t pos = 0) {
    iobuf_.copy_to(dest, n, pos);
  }

  void CopyTo(char* dest, size_t n = (size_t)-1L, size_t pos = 0) const {
    iobuf_.copy_to(dest, n, pos);
  }

  void CopyTo(IOBuffer* dest, size_t n = (size_t)-1L, size_t pos = 0) const {
    iobuf_.copy_to(&dest->iobuf_, n, pos);
  }

  size_t AppendTo(IOBuffer* buffer, size_t n = (size_t)-1L, size_t pos = 0) {
    return iobuf_.append_to(&buffer->IOBuf(), n, pos);
  }

  void Append(const IOBuffer* buffer) { iobuf_.append(buffer->iobuf_); }

  void AppendV(const std::vector<iovec>& iovs) {
    iobuf_.appendv((const const_iovec*)iovs.data(), iovs.size());
  }

  void AppendUserData(void* data, size_t size,
                      std::function<void(void*)> deleter) {
    iobuf_.append_user_data(data, size, deleter);
  }

  void PopFront(size_t n) { iobuf_.pop_front(n); }
  void PopBack(size_t n) { iobuf_.pop_back(n); }

  size_t Size() const { return iobuf_.length(); }

  char* Fetch1() const {
    CHECK_EQ(iobuf_.backing_block_num(), 1);
    return (char*)iobuf_.fetch1();
  }

  std::vector<iovec> Fetch() const {
    std::vector<iovec> iovecs;
    for (int i = 0; i < iobuf_.backing_block_num(); i++) {
      const auto& string_piece = iobuf_.backing_block(i);

      char* data = (char*)string_piece.data();
      size_t size = string_piece.length();
      iovecs.emplace_back(iovec{data, size});
    }
    return iovecs;
  }

  std::string Describe() const {
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

 private:
  butil::IOBuf iobuf_;
};

}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_IO_BUFFER_H_
