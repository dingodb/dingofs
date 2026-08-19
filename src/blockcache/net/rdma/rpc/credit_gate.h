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

#ifndef DINGOFS_BLOCKCACHE_NET_RDMA_RPC_CREDIT_GATE_H_
#define DINGOFS_BLOCKCACHE_NET_RDMA_RPC_CREDIT_GATE_H_

#include <coroutine>
#include <cstdint>

#include "blockcache/core/reactor/reactor.h"
#include "blockcache/utils/containers/park_queue.h"

namespace dingofs {
namespace blockcache {

// Flow control: 1 credit = one posted, unconsumed peer receive buffer.
// One credit is held in reserve exclusively for credit-grant messages.
class CreditGate {
 public:
  // co_await -> void. Parks in arrival order when no credit is available.
  class Consume {
   public:
    explicit Consume(CreditGate* gate) : gate_(gate) {}

    bool await_ready() const noexcept { return gate_->regular_ > 0; }

    template <TaskPromise P>
    void await_suspend(std::coroutine_handle<P> h) noexcept {
      task_ = &h.promise();
      gate_->parked_.Push(this);
    }

    void await_resume() noexcept {
      // Ready path takes the credit now; a parked one got it in Refill.
      if (!taken_ && !gate_->failed_) {
        --gate_->regular_;
      }
    }

    Consume* park_next = nullptr;

   private:
    friend class CreditGate;

    CreditGate* gate_;
    Task* task_ = nullptr;
    bool taken_ = false;
  };

  void Init(uint32_t peer_recv_buffers) {
    regular_ = peer_recv_buffers > 0 ? peer_recv_buffers - 1 : 0;
    reserve_ = peer_recv_buffers > 0 ? 1 : 0;
  }

  Consume ConsumeOne() { return Consume(this); }

  // Pings only: take a regular credit if free, never park; dead conn = false.
  bool TryConsumeOne() {
    if (failed_ || regular_ == 0) {
      return false;
    }
    --regular_;
    return true;
  }

  bool TakeReserveOrDead() {
    if (failed_) {
      return true;  // let the caller fail on the broken connection instead
    }
    if (reserve_ == 0) {
      return false;
    }
    --reserve_;
    return true;
  }

  // The peer told us it freed n buffers.
  void Refill(uint32_t n) {
    if (n == 0) {
      return;
    }
    if (reserve_ == 0) {
      reserve_ = 1;
      --n;
    }
    // Credits go straight to waiters so a late arrival cannot overtake them.
    while (n > 0) {
      Consume* waiter = parked_.Pop();
      if (waiter == nullptr) {
        break;
      }
      waiter->taken_ = true;
      ThisReactor().Schedule(waiter->task_);
      --n;
    }
    regular_ += n;
  }

  void FailAll() {
    failed_ = true;
    parked_.TakeAllAnd([](Consume* waiter) {
      waiter->taken_ = true;
      ThisReactor().Schedule(waiter->task_);
    });
  }

 private:
  friend class Consume;

  uint32_t regular_ = 0;
  uint32_t reserve_ = 0;
  bool failed_ = false;
  ParkQueue<Consume> parked_;
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_NET_RDMA_RPC_CREDIT_GATE_H_
