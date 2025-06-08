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

#include <vector>

namespace dingofs {

IOBuffer::IOBuffer(butil::IOBuf iobuf) : iobuf_(iobuf) {}

IOBuffer::IOBuffer(const char* data, size_t size) { iobuf_.append(data, size); }

butil::IOBuf& IOBuffer::IOBuf() { return iobuf_; }

std::vector<iovec> IOBuffer::Fetch() {
  std::vector<iovec> iovecs;
  // for (int i = 0; i < iobuf_.block_count(); i++) { // FIXME
  for (int i = 0; i < iobuf_.backing_block_num(); i++) {
    const auto& string_piece = iobuf_.backing_block(i);

    char* data = (char*)string_piece.data();
    size_t size = string_piece.length();
    iovecs.emplace_back(iovec{data, size});
  }
  return iovecs;
}

void IOBuffer::CopyTo(char* dest) { iobuf_.copy_to(dest); }

size_t IOBuffer::Size() const { return iobuf_.length(); }

}  // namespace dingofs
