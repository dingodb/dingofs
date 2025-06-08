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

#ifndef DINGOFS_SRC_CACHE_STORAGE_AIO_AIO_H_
#define DINGOFS_SRC_CACHE_STORAGE_AIO_AIO_H_

#include "cache/common/common.h"
#include "cache/storage/closure.h"
#include "cache/utils/phase_timer.h"
#include "common/io_buffer.h"

namespace dingofs {
namespace cache {

enum class AioType : uint8_t {
  kRead = 0,
  kWrite = 1,
};

struct AioClosure : public Closure {
  AioClosure(AioType iotype, int fd, off_t offset, size_t length,
             const IOBuffer& buffer_in, IOBuffer* buffer_out)
      : iotype(iotype),
        fd(fd),
        offset(offset),
        length(length),
        buffer_in(buffer_in),
        buffer_out(buffer_out) {}

  void Run() override {
    std::unique_lock<BthreadMutex> lk(mutex);
    cond.notify_all();
  }

  void Wait() {
    std::unique_lock<BthreadMutex> lk(mutex);
    cond.wait(lk);
  }

  std::string ToString() const {
    return absl::StrFormat("aio(%s,%d,%lld,%zu)",
                           iotype == AioType::kRead ? "read" : "write", fd,
                           offset, length);
  }

  AioType iotype;
  int fd;
  off_t offset;
  size_t length;
  IOBuffer buffer_in;
  IOBuffer* buffer_out;
  PhaseTimer timer;
  void* ctx;  // for different io ring
  BthreadMutex mutex;
  BthreadConditionVariable cond;
};

inline AioClosure AioWriteClosure(int fd, const IOBuffer& buffer) {
  return AioClosure(AioType::kWrite, fd, 0, buffer.Size(), buffer, nullptr);
}

inline AioClosure AioReadClosure(int fd, off_t offset, size_t length,
                                 IOBuffer* buffer) {
  return AioClosure(AioType::kRead, fd, offset, length, IOBuffer(), buffer);
}

inline bool IsAioRead(AioClosure* aio) { return aio->iotype == AioType::kRead; }

inline bool IsAioWrite(AioClosure* aio) {
  return aio->iotype == AioType::kWrite;
}

class IORing {
 public:
  virtual ~IORing() = default;

  virtual Status Init() = 0;
  virtual Status Shutdown() = 0;

  virtual Status PrepareIO(AioClosure* aio) = 0;
  virtual Status SubmitIO() = 0;
  virtual Status WaitIO(uint64_t timeout_ms,
                        std::vector<AioClosure*>* aios) = 0;

  virtual uint32_t GetIODepth() const = 0;
};

using IORingSPtr = std::shared_ptr<IORing>;

class AioQueue {
 public:
  virtual ~AioQueue() = default;

  virtual Status Init() = 0;
  virtual Status Shutdown() = 0;

  virtual void Submit(AioClosure* aio) = 0;
};

using AioQueueUPtr = std::unique_ptr<AioQueue>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_STORAGE_AIO_AIO_H_
