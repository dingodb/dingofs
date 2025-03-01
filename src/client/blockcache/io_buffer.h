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

#ifndef DINGOFS_SRC_CLIENT_BLOCKCACHE_IOBUF_BUFFER_H_
#define DINGOFS_SRC_CLIENT_BLOCKCACHE_IOBUF_BUFFER_H_

#include <butil/iobuf.h>

#include <map>

namespace dingofs {
namespace client {
namespace blockcache {

struct RawBuffer {
  RawBuffer(char* data, size_t size) : data(data), size(size) {};

  char* data;
  size_t size;
};

// A continuous buffer
class IOBuffer {
 public:
  virtual ~IOBuffer() = default;

  // like memset():
  //   offset: dest offset
  //   n: data size
  //   c: initialize character, default is '\0'
  virtual void Set(off_t offset, int n, char c = '\0') = 0;

  // like memcpy():
  //   offset: dest offset
  //   data: source buffer
  //   n: data size
  virtual void Copy(off_t offset, void* data, size_t n) = 0;

  // copy data into current offset which can modify by Seek(),
  // default current offset is 0
  virtual void Copy(void* data, size_t n) = 0;

  // Append iobuf into specified offset without copying
  virtual void Append(off_t offset, const butil::IOBuf& iobuf) = 0;

  // seek buffer current offset to specified offset
  virtual void Seek(off_t offset = 0);

  virtual size_t Size() = 0;

  // Is buffer continuous?
  virtual bool Check() = 0;

  virtual std::vector<RawBuffer> Fetch() = 0;
};

// Wrap IOBuf as continuous buffer
// NOTE:
//  (1) can't copy same offset twice
//  (2) not thread-safe
class IOBufferImpl : public IOBuffer {
 public:
  IOBufferImpl();

  void Set(off_t offset, int n, char c = '\0') override;

  void Copy(off_t offset, void* data, size_t n) override;

  void Copy(void* data, size_t n) override;

  void Append(off_t offset, const butil::IOBuf& iobuf) override;

  void Seek(off_t offset = 0) override;

  size_t Size() override;

  bool Check() override;

  std::vector<RawBuffer> Fetch() override;

 private:
  off_t cur_offset_;                    // current offset
  std::map<int, butil::IOBuf> iobufs_;  // key: offset
};

}  // namespace blockcache
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_BLOCKCACHE_IOBUF_BUFFER_H_
