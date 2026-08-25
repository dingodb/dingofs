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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_FLOW_CONTROL_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_FLOW_CONTROL_H_

#include <glog/logging.h>

#include <algorithm>
#include <coroutine>
#include <cstdint>
#include <memory>

#include "blockcache/core/reactor/reactor.h"
#include "blockcache/utils/containers/park_queue.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

class CreditFlowControl {
 public:
  class Waiter {
   public:
    Waiter(const Waiter&) = delete;
    Waiter& operator=(const Waiter&) = delete;

    bool await_ready() const noexcept {
      return flow_control_->shutdown_ ||
             flow_control_->available_send_credits_ > kCreditReturnReserve;
    }

    template <TaskPromise P>
    void await_suspend(std::coroutine_handle<P> h) noexcept {
      task_ = &h.promise();
      flow_control_->parked_.Push(this);
    }

    bool await_resume() noexcept {
      return acquired_ ? !flow_control_->shutdown_
                       : flow_control_->TryAcquireSendCredit();
    }

    Waiter* park_next = nullptr;

   private:
    friend class CreditFlowControl;

    explicit Waiter(CreditFlowControl* flow_control)
        : flow_control_(flow_control) {}

    CreditFlowControl* flow_control_;
    Task* task_ = nullptr;
    bool acquired_ = false;  // AddSendCredits assigned one while I was parked
  };

  CreditFlowControl(uint16_t max_send_credits, uint16_t recv_queue_depth)
      : available_send_credits_(max_send_credits),
        max_send_credits_(max_send_credits),
        credit_return_threshold_((recv_queue_depth + 1) / 2) {
    DCHECK_GT(available_send_credits_, kCreditReturnReserve)
        << "a connection that can never send";
  }

  CreditFlowControl(const CreditFlowControl&) = delete;
  CreditFlowControl& operator=(const CreditFlowControl&) = delete;

  Waiter AcquireSendCredit() { return Waiter(this); }

  bool TryAcquireSendCredit() {
    return TryConsumeSendCredit(kCreditReturnReserve);
  }

  bool TryAcquireForCreditReturn() {
    return credits_to_return_ >= credit_return_threshold_ &&
           TryConsumeSendCredit(/*reserved_credits=*/0);
  }

  uint16_t TakeCreditsToReturn() {
    DCHECK_LE(credits_to_return_, uint32_t{UINT16_MAX});
    const uint32_t credits = std::min<uint32_t>(credits_to_return_, UINT16_MAX);
    credits_to_return_ -= credits;
    return static_cast<uint16_t>(credits);
  }

  void RestoreCreditsToReturn(uint16_t credits) {
    credits_to_return_ += credits;
  }

  void AddSendCredits(uint16_t credits) {
    if (shutdown_) {
      return;
    }

    available_send_credits_ = std::min<uint32_t>(
        available_send_credits_ + credits, max_send_credits_);
    while (!parked_.empty() && TryConsumeSendCredit(kCreditReturnReserve)) {
      Waiter* waiter = parked_.Pop();
      waiter->acquired_ = true;
      NotifyWaiter(waiter);
    }
  }

  void Shutdown() {
    shutdown_ = true;
    parked_.TakeAllAnd([this](Waiter* waiter) { NotifyWaiter(waiter); });
  }

 private:
  static constexpr uint32_t kCreditReturnReserve = 1;

  bool TryConsumeSendCredit(uint32_t reserved_credits) {
    if (shutdown_ || available_send_credits_ <= reserved_credits) {
      return false;
    }
    --available_send_credits_;
    return true;
  }

  void NotifyWaiter(Waiter* waiter) { ThisReactor().Schedule(waiter->task_); }

  bool shutdown_ = false;
  uint32_t available_send_credits_;
  const uint32_t max_send_credits_;
  uint32_t credits_to_return_ = 0;
  const uint32_t credit_return_threshold_;  // half our ring, rounded up
  ParkQueue<Waiter> parked_;
};

using CreditFlowControlUPtr = std::unique_ptr<CreditFlowControl>;

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_FLOW_CONTROL_H_
