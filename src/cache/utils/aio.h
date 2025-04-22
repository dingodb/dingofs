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
 * Created Date: 2025-04-09
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_UTILS_AIO_H_
#define DINGOFS_SRC_CACHE_UTILS_AIO_H_

#include <brpc/closure_guard.h>
#include <butil/iobuf.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/uio.h>

#include <cstddef>
#include <cstdint>

#include "cache/common/common.h"
#include "cache/utils/phase_timer.h"

namespace dingofs {
namespace cache {
namespace utils {

class Closure : public ::google::protobuf::Closure {
 public:
  void SetStatus(Status status) { status_ = status; }

  Status GetStatus() const { return status_; }

 private:
  Status status_;
};

enum class AioType : uint8_t {
  kRead = 0,
  kWrite = 0,
};

struct Aio {
  Aio(AioType aio_type, int fd, off_t offset, size_t length, char* buffer,
      Closure* done)
      : aio_type(aio_type),
        fd(fd),
        offset(offset),
        length(length),
        buffer(buffer),
        done(done) {}

  AioType aio_type;
  int fd;
  off_t offset;
  size_t length;
  char* buffer;               // TODO: zerocopy
  char* iov_buffer{nullptr};  // only for 3fs usrbio
  int retcode{-1};
  Closure* done;
  PhaseTimer timer;
};

inline std::string StrAioType(AioType aio_type) {
  return aio_type == AioType::kRead ? "read" : "write";
}

inline std::string StrAio(Aio* aio) {
  return absl::StrFormat("aio(%s,%d,%d,%d)", StrAioType(aio->aio_type), aio->fd,
                         aio->offset, aio->length);
}

inline Aio AioRead(int fd, off_t offset, size_t length, char* buffer,
                   Closure* done) {
  return Aio(AioType::kRead, fd, offset, length, buffer, done);
}

inline Aio AioWrite(int fd, off_t offset, size_t length, char* buffer,
                    Closure* done) {
  return Aio(AioType::kWrite, fd, offset, length, buffer, done);
}

class IoRing {
 public:
  virtual ~IoRing() = default;

  virtual Status Init(uint32_t io_depth) = 0;

  virtual void Shutdown() = 0;

  virtual Status PrepareIo(Aio* aio) = 0;

  virtual Status SubmitIo() = 0;

  virtual Status WaitIo(uint32_t timeout_ms, std::vector<Aio*>* aios) = 0;

  virtual void PostIo(Aio* aio) = 0;
};

class AioQueue {
 public:
  virtual ~AioQueue() = default;

  virtual Status Init(uint32_t io_depth) = 0;

  virtual Status Shutdown() = 0;

  virtual void Submit(Aio* aio) = 0;
};

}  // namespace utils
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_UTILS_AIO_H_
