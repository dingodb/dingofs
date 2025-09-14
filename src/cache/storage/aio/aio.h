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

#include <absl/strings/str_format.h>
#include <bits/types/struct_iovec.h>

#include "cache/common/type.h"
#include "cache/storage/closure.h"
#include "cache/utils/context.h"
#include "cache/utils/step_timer.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

struct Aio : public Closure {
  Aio(ContextSPtr ctx, int fd, off_t offset, size_t length, IOBuffer* buffer,
      bool for_read, int fixed_buffer_index = -1)
      : ctx(ctx),
        fd(fd),
        offset(offset),
        length(length),
        buffer(buffer),
        for_read(for_read),
        fixed_buffer_index(fixed_buffer_index) {
    CHECK_GT(fd, 0);
    CHECK_GE(offset, 0);
    CHECK_GT(length, 0);
    CHECK_NOTNULL(buffer);
  }

  std::string ToString() const {
    return absl::StrFormat("%s(%d,%lld,%zu)", for_read ? "read" : "write", fd,
                           offset, length);
  }

  void Wait() {
    std::unique_lock<BthreadMutex> lk(mutex);
    while (!finish_) {
      cond.wait(lk);
    }

    VLOG(9) << "Aio wait over: aio = " << ToString();
  }

  void Run() override {
    std::lock_guard<BthreadMutex> lk(mutex);
    VLOG(9) << "Aio run over: aio = " << ToString();
    finish_ = true;
    cond.notify_one();
  }

  ContextSPtr ctx;
  StepTimer timer;
  int fd;
  off_t offset;
  size_t length;
  IOBuffer* buffer;
  bool for_read;
  int fixed_buffer_index;     // for write fixed
  std::vector<iovec> iovecs;  // for writev
  bool finish_{false};
  BthreadMutex mutex;
  BthreadConditionVariable cond;
};

class IORing {
 public:
  virtual ~IORing() = default;

  virtual Status Start() = 0;
  virtual Status Shutdown() = 0;

  virtual Status PrepareIO(Aio* aio) = 0;
  virtual Status SubmitIO() = 0;
  virtual Status WaitIO(uint64_t timeout_ms,
                        std::vector<Aio*>* completed_aios) = 0;
};

using IORingSPtr = std::shared_ptr<IORing>;

class AioQueue {
 public:
  virtual ~AioQueue() = default;

  virtual Status Start() = 0;
  virtual Status Shutdown() = 0;

  virtual void Submit(Aio* aio) = 0;
};

using AioQueueUPtr = std::unique_ptr<AioQueue>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_STORAGE_AIO_AIO_H_
