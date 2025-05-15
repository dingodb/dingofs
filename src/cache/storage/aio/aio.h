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

#include <brpc/closure_guard.h>
#include <butil/iobuf.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/uio.h>

#include <cstddef>
#include <cstdint>

#include "cache/common/common.h"
#include "cache/common/types.h"
#include "cache/storage/buffer.h"
#include "cache/storage/closure.h"
#include "cache/utils/phase_timer.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace storage {

enum class AioType : uint8_t {
  kRead = 0,
  kWrite = 1,
};

class AioClosure : public Closure {
 public:
  using Phase = dingofs::cache::utils::Phase;
  using PhaseTimer = dingofs::cache::utils::PhaseTimer;

  using OnCompeteFunc = std::function<void(Status)>;

  AioClosure(AioType iotype, int fd, off_t offset, size_t length,
             IOBuffer* buffer)
      : iotype_(iotype),
        fd_(fd),
        offset_(offset),
        length_(length),
        buffer_(buffer),
        ctx_(nullptr) {}

  void Run() override {
    {
      std::unique_lock<bthread::Mutex> lk(mutex_);
      cond_.notify_all();
    }
    delete this;
  }

  void Wait() {
    std::unique_lock<bthread::Mutex> lk(mutex_);
    cond_.wait(lk);
  }

  std::string ToString() {
    return absl::StrFormat("aio(%s,%d,%d,%d)",
                           iotype_ == AioType::kRead ? "read" : "write", fd_,
                           offset_, length_);
  }

  // utils
  bool IsRead() { return iotype_ == AioType::kRead; };
  bool IsWrite() { return iotype_ == AioType::kWrite; };
  int Fd() const { return fd_; }
  off_t Offset() const { return offset_; }
  size_t Length() const { return length_; }
  IOBuffer* Buffer() { return buffer_; }
  PhaseTimer& Timer() { return timer_; }
  void NextPhase(Phase phase) { timer_.NextPhase(phase); }
  Phase GetPhase() { return timer_.CurrentPhase(); }
  void SetCtx(void* ctx) { ctx_ = ctx; }
  void* GetCtx() { return ctx_; }

 private:
  AioType iotype_;
  int fd_;
  off_t offset_;
  size_t length_;
  IOBuffer* buffer_;
  PhaseTimer timer_;                 // internal
  void* ctx_;                        // internal
  bthread::Mutex mutex_;             // internal
  bthread::ConditionVariable cond_;  // internal
};

using Aios = std::vector<AioClosure*>;

class IORing {
 public:
  virtual ~IORing() = default;
  virtual uint32_t GetIODepth() = 0;

  virtual Status Init() = 0;
  virtual Status Shutdown() = 0;

  virtual Status PrepareIO(AioClosure* aio) = 0;
  virtual Status SubmitIO() = 0;
  virtual Status WaitIO(uint32_t timeout_ms, Aios* aios) = 0;
};

class AioQueue {
 public:
  virtual ~AioQueue() = default;

  virtual Status Init() = 0;
  virtual Status Shutdown() = 0;

  virtual void Submit(AioClosure* aio) = 0;
};

}  // namespace storage
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_STORAGE_AIO_AIO_H_
