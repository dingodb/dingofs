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

#include "client/vfs/data_buffer.h"

#include <butil/iobuf.h>

#include "common/io_buffer.h"

namespace dingofs {
namespace client {
namespace vfs {

DataBuffer::DataBuffer() : io_buffer_(new class IOBuffer()) {}

DataBuffer::~DataBuffer() {
  if (io_buffer_ != nullptr) {
    delete io_buffer_;
    io_buffer_ = nullptr;
  }
}

IOBuffer* DataBuffer::RawIOBuffer() { return io_buffer_; }

std::vector<IOVec> DataBuffer::GatherIOVecs() const {
  std::vector<IOVec> iovecs;

  const butil::IOBuf& iobuf = io_buffer_->IOBuf();
  auto tmp_iovecs = io_buffer_->Fetch();

  iovecs.reserve(tmp_iovecs.size());
  for (const auto& iov : tmp_iovecs) {
    iovecs.emplace_back(IOVec{(char*)iov.iov_base, iov.iov_len});
  }

  return iovecs;
}

std::string DataBuffer::Describe() const { return io_buffer_->Describe(); }

}  // namespace vfs
}  // namespace client
}  // namespace dingofs