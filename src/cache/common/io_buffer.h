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
 * Created Date: 2025-03-12
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_COMMON_IO_BUFFER_H_
#define DINGOFS_SRC_CACHE_COMMON_IO_BUFFER_H_

#include <butil/iobuf.h>

namespace dingofs {
namespace cache {
namespace common {

struct BufferVec {
  char* base;
  size_t length;
};

class IOBuffer {
 public:
  virtual ~IOBuffer() = default;

  virtual void AppendUserData(char* data, size_t length,
                              std::function<void(void*)> deleter) = 0;

  virtual size_t AppendTo(butil::IOBuf* buf, size_t n = (size_t)-1L,
                          size_t pos = 0) = 0;

  virtual size_t AppendTo(IOBuffer*, size_t n = (size_t)-1L,
                          size_t pos = 0) = 0;
};

class IOBufBuffer : public IOBuffer {
 public:
  IOBufBuffer() = default;

  void AppendUserData(char* data, size_t length,
                      std::function<void(void*)> deleter) override;

  size_t AppendTo(butil::IOBuf* buf, size_t n = (size_t)-1L,
                  size_t pos = 0) override;

  size_t AppendTo(IOBuffer* buf, size_t n = (size_t)-1L,
                  size_t pos = 0) override;

 private:
  butil::IOBuf iobuf_;
};

}  // namespace common
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_COMMON_IO_BUFFER_H_
