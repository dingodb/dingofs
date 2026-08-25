/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_SEND_BUFFER_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_SEND_BUFFER_H_

#include <coroutine>
#include <cstdint>
#include <memory>
#include <vector>

#include "blockcache/core/reactor/reactor.h"
#include "blockcache/infiniband/base/buffer_pool.h"
#include "blockcache/utils/containers/park_queue.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

struct alignas(8) SendBuffer {
  char* data = nullptr;
  uint32_t capacity = 0;
  uint32_t length = 0;
  void* conn = nullptr;
  SendBuffer* next = nullptr;
};

class SendBufferPool {
 public:
  // co_await -> SendBuffer*, or nullptr once the connection is broken.
  class Waiter {
   public:
    Waiter(const Waiter&) = delete;
    Waiter& operator=(const Waiter&) = delete;

    bool await_ready() const noexcept { return pool_->free_ != nullptr; }

    template <TaskPromise P>
    void await_suspend(std::coroutine_handle<P> h) noexcept {
      task_ = &h.promise();
      pool_->parked_.Push(this);
    }

    SendBuffer* await_resume() noexcept {
      return buffer_ != nullptr ? buffer_ : pool_->TryAcquire();
    }

    Waiter* park_next = nullptr;

   private:
    friend class SendBufferPool;

    explicit Waiter(SendBufferPool* pool) : pool_(pool) {}

    SendBufferPool* pool_;
    SendBuffer* buffer_ = nullptr;
    Task* task_ = nullptr;
  };

  explicit SendBufferPool(BufferPool* pool) : pool_(pool) {}
  ~SendBufferPool();

  SendBufferPool(const SendBufferPool&) = delete;
  SendBufferPool& operator=(const SendBufferPool&) = delete;

  Status Init(uint32_t buffer_size, uint32_t buffer_count, void* conn);

  SendBuffer* TryAcquire() {
    SendBuffer* buffer = free_;
    if (buffer != nullptr) {
      free_ = buffer->next;
      buffer->next = nullptr;
    }
    return buffer;
  }

  Waiter Acquire() { return Waiter(this); }

  void Release(SendBuffer* buffer) {
    Waiter* waiter = parked_.Pop();
    if (waiter != nullptr) {
      waiter->buffer_ = buffer;
      NotifyWaiter(waiter);
      return;
    }
    buffer->next = free_;
    free_ = buffer;
  }

  void FailAll() {
    parked_.TakeAllAnd([this](Waiter* waiter) {
      waiter->buffer_ = nullptr;
      NotifyWaiter(waiter);
    });
  }

  uint32_t buffer_size() const { return buffer_size_; }
  uint32_t lkey() const { return pool_->lkey(); }

 private:
  void NotifyWaiter(Waiter* waiter) { ThisReactor().Schedule(waiter->task_); }

  BufferPool* pool_;
  uint32_t buffer_size_ = 0;
  std::vector<SendBuffer> buffers_;
  SendBuffer* free_ = nullptr;
  ParkQueue<Waiter> parked_;
};

using SendBufferPoolUPtr = std::unique_ptr<SendBufferPool>;

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_SEND_BUFFER_H_
