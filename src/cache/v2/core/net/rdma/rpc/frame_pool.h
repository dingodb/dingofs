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

#ifndef DINGOFS_CACHE_V2_CORE_NET_RDMA_RPC_FRAME_POOL_H_
#define DINGOFS_CACHE_V2_CORE_NET_RDMA_RPC_FRAME_POOL_H_

#include <coroutine>
#include <cstdint>
#include <vector>

#include "cache/v2/core/memory/slab_pool.h"
#include "cache/v2/core/reactor/reactor.h"
#include "cache/v2/utils/containers/park_queue.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace v2 {

class RdmaConnection;

// A frame buffer in flight; carries its connection for wr-id routing.
struct alignas(8) FrameBuf {
  RdmaConnection* conn = nullptr;
  char* frame = nullptr;
  FrameBuf* next = nullptr;
};

// Per-connection pool of registered frame buffers.
// A buffer is busy until its send completion; exhaustion parks the caller.
class FramePool {
 public:
  // co_await -> FrameBuf*, or nullptr once the connection is broken.
  class Get {
   public:
    explicit Get(FramePool* pool) : pool_(pool) {}

    bool await_ready() const noexcept { return pool_->free_ != nullptr; }

    template <TaskPromise P>
    void await_suspend(std::coroutine_handle<P> h) noexcept {
      task_ = &h.promise();
      pool_->parked_.Push(this);
    }

    FrameBuf* await_resume() noexcept {
      return buffer_ != nullptr ? buffer_ : pool_->TryGet();
    }

    Get* park_next = nullptr;

   private:
    friend class FramePool;

    FramePool* pool_;
    Task* task_ = nullptr;
    FrameBuf* buffer_ = nullptr;
  };

  ~FramePool();

  // Buffers come from the shard's slab pool, under one memory region.
  Status Init(RdmaConnection* conn, SlabPool* pool, uint32_t lkey,
              uint32_t count, uint32_t frame_bytes);

  FrameBuf* TryGet() {
    FrameBuf* buffer = free_;
    if (buffer != nullptr) {
      free_ = buffer->next;
      buffer->next = nullptr;
    }
    return buffer;
  }

  Get GetBuf() { return Get(this); }

  void Put(FrameBuf* buffer) {
    Get* waiter = parked_.Pop();
    if (waiter != nullptr) {
      waiter->buffer_ = buffer;
      ThisReactor().Schedule(waiter->task_);
      return;
    }
    buffer->next = free_;
    free_ = buffer;
  }

  void FailAll() {
    parked_.TakeAllAnd([](Get* waiter) {
      waiter->buffer_ = nullptr;
      ThisReactor().Schedule(waiter->task_);
    });
  }

  uint32_t frame_bytes() const { return frame_bytes_; }
  uint32_t lkey() const { return lkey_; }

 private:
  friend class Get;

  SlabPool* pool_ = nullptr;  // buffers_[].frame goes back here
  std::vector<FrameBuf> buffers_;
  FrameBuf* free_ = nullptr;
  uint32_t frame_bytes_ = 0;
  uint32_t lkey_ = 0;
  ParkQueue<Get> parked_;
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_NET_RDMA_RPC_FRAME_POOL_H_
