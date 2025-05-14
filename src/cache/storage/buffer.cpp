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

#include "cache/storage/buffer.h"

#include <butil/iobuf.h>

namespace dingofs {
namespace cache {
namespace storage {

IOBuffer::IOBuffer(butil::IOBuf iobuf) : iobuf_(iobuf) {}

butil::IOBuf& IOBuffer::IOBuf() { return iobuf_; }

BufferVec IOBuffer::Buffers() {
  std::string::value_type* c;

  BufferVec bufvec;
  for (int i = 0; i < iobuf_.block_count(); i++) {
    const auto& string_piece = iobuf_.backing_block(i);

    char* data = (char*)string_piece.data();
    size_t size = string_piece.length();
    bufvec.emplace_back(RawBuffer(data, size));
  }
  return bufvec;
}

void IOBuffer::CopyTo(char* buffer) { iobuf_.copy_to(buffer); }

size_t IOBuffer::Size() { return iobuf_.length(); }

}  // namespace storage
}  // namespace cache
}  // namespace dingofs
