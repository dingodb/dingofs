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
 * Created Date: 2025-03-01
 * Author: Jingli Chen (Wine93)
 */

#include <butil/iobuf.h>
#include <glog/logging.h>

#include <algorithm>
#include <vector>

#include "client/blockcache/io_buffer.h"

namespace dingofs {
namespace client {
namespace blockcache {

IOBufferImpl::IOBufferImpl() : cur_offset_(0) {}

void IOBufferImpl::Set(off_t offset, int n, char c) {
  butil::IOBuf iobuf;
  iobuf.resize(n, c);
  Append(offset, iobuf);
}

void IOBufferImpl::Copy(off_t offset, void* data, size_t n) {
  butil::IOBuf iobuf;
  iobuf.append(data, n);  // copying
  Append(offset, iobuf);
}

void IOBufferImpl::Copy(void* data, size_t n) { Copy(cur_offset_, data, n); }

void IOBufferImpl::Append(off_t offset, const butil::IOBuf& iobuf) {
  auto iter = iobufs_.find(offset);
  CHECK(iter == iobufs_.end()) << "can't copy same buffer twice";
  iobufs_[offset] = iobuf;
}

void IOBufferImpl::Seek(off_t offset) { cur_offset_ = offset; }

size_t IOBufferImpl::Size() {
  size_t size = 0;
  for (auto& item : iobufs_) {
    size += item.second.size();
  }
  return size;
}

bool IOBufferImpl::Check() {
  off_t last_offset = -1;
  for (auto& item : iobufs_) {
    if (item.first <= last_offset) {
      LOG(ERROR) << "offset not continuous, last_offset = " << last_offset
                 << ", current_offset = " << item.first;
      return false;
    }
    last_offset = item.first + item.second.length();
  }
  return true;
}

std::vector<RawBuffer> IOBufferImpl::Fetch() {
  CHECK(Check());
  std::vector<RawBuffer> raw_buffers;
  for (auto& item : iobufs_) {
    auto& iobuf = item.second;
    CHECK_EQ(iobuf.block_count(), 1);
    raw_buffers.emplace_back(RawBuffer((char*)iobuf.fetch1(), iobuf.size()));
  }
}

}  // namespace blockcache
}  // namespace client
}  // namespace dingofs
