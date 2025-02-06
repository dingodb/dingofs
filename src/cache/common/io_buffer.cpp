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
 * Created Date: 2025-02-27
 * Author: Jingli Chen (Wine93)
 */

#include "cache/common/io_buffer.h"

namespace dingofs {
namespace cache {
namespace common {

IOBufBuffer::AppendUserData(char* data, size_t length,
                            std::function<void(void*)> deleter) {
  iobuf_.append_user_data(data, length, deleter);
}

size_t IOBufBuffer::AppendTo(butil::IOBuf* buf, size_t n, size_t pos) {
  return iobuf_.append_to(buf, n, pos);
}

size_t IOBufBuffer::AppendTo(IOBuffer* buf, size_t n, size_t pos) {
  return iobuf_.append(buf.iobuf_, n, pos);
}

}  // namespace common
}  // namespace cache
}  // namespace dingofs
