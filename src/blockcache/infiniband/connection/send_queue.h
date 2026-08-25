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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_SEND_QUEUE_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_SEND_QUEUE_H_

#include <infiniband/verbs.h>

#include <cstdint>

#include "blockcache/common/status.h"
#include "blockcache/core/reactor/io_awaiter.h"
#include "blockcache/infiniband/base/queue_pair.h"
#include "blockcache/infiniband/common/wr_id.h"
#include "blockcache/infiniband/connection/plug.h"
#include "blockcache/utils/containers/park_queue.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

class OpAwaiter;
class BatchAwaiter;

class SendQueue : public Pluggable {
 public:
  SendQueue() = default;

  SendQueue(QueuePair* queue_pair, uint32_t depth);
  ~SendQueue() override;

  SendQueue(const SendQueue&) = delete;
  SendQueue& operator=(const SendQueue&) = delete;
  SendQueue(SendQueue&&) noexcept = default;
  SendQueue& operator=(SendQueue&&) noexcept = default;

  void Start();
  void Shutdown();

  void Submit(OpAwaiter* op);
  bool TrySubmit(ibv_send_wr* wr);
  void SubmitBatch(BatchAwaiter* batch);

  uint32_t AvailableWrs(bool is_read) const;
  void Release(uint32_t completed_wrs, uint32_t completed_read_wrs);

  uint32_t inflights() const { return total_inflights_; }
  uint32_t max_inline_data() const { return max_inline_data_; }

 private:
  static constexpr uint32_t kReadOversubscriptionFactor = 8;

  // throttle
  bool TryAcquire(bool is_read);
  void Acquire(uint32_t num_wrs, uint32_t num_read_wrs);
  void Countdown(uint32_t num_wrs, uint32_t num_read_wrs);
  OpAwaiter* TryAdmitWaiter();

  // post or enqueue
  ibv_send_wr* PostOrEnqueue(ibv_send_wr* wr);
  ibv_send_wr* PostWorkRequests(ibv_send_wr* first_wr);
  void Enqueue(ibv_send_wr* first_wr, ibv_send_wr* last_wr);

  // flush
  void Unplug() override;
  void Flush();
  ibv_send_wr* TakeAll();

  // fail
  void HandlePostFailure(ibv_send_wr* first_wr, ibv_send_wr* first_unposted_wr);
  static void HandleAllFailure(ibv_send_wr* wr, int32_t wc_status);
  static void HandleOne(ibv_send_wr* wr, int32_t wc_status);
  void FailWaiters(int32_t wc_status);

  bool has_waiters() const {
    return !waiters_.empty() || !read_waiters_.empty();
  }

  bool running_ = false;
  ibv_qp* qp_ = nullptr;
  uint32_t max_inline_data_ = 0;
  uint32_t max_total_inflights_ = 0;
  uint32_t max_read_inflights_ = 0;
  uint32_t total_inflights_ = 0;
  uint32_t read_inflights_ = 0;
  ibv_send_wr* pending_wr_head_ = nullptr;
  ibv_send_wr* pending_wr_tail_ = nullptr;
  ParkQueue<OpAwaiter> waiters_;
  ParkQueue<OpAwaiter> read_waiters_;
};

struct SendWorkRequest {
  ibv_sge sge{};
  ibv_send_wr wr{};

  SendWorkRequest() { wr.sg_list = &sge; }

  SendWorkRequest(const SendWorkRequest& o) : sge(o.sge), wr(o.wr) {
    wr.sg_list = &sge;
  }

  SendWorkRequest& operator=(const SendWorkRequest& o) {
    sge = o.sge;
    wr = o.wr;
    wr.sg_list = &sge;
    return *this;
  }
};

class OpAwaiter final : public IoAwaiter<OpAwaiter> {
 public:
  OpAwaiter(SendQueue* queue, SendWorkRequest work_request) noexcept
      : queue_(queue), work_request_(work_request) {
    ibv_send_wr* wr = &work_request_.wr;
    wr->wr_id = MakeWrId(this, kTagOp);
    wr->send_flags |= IBV_SEND_SIGNALED;
  }

  OpAwaiter(const OpAwaiter&) = delete;
  OpAwaiter& operator=(const OpAwaiter&) = delete;

  Status await_resume() const {
    return ToStatus(static_cast<ibv_wc_status>(result_),
                    "complete rdma operation");
  }

  void Arm() { queue_->Submit(this); }

  void OnComplete(int32_t wc_status) noexcept {
    queue_->Release(1, is_read() ? 1 : 0);
    ResumeLater(wc_status);
  }

  void Abort(int32_t wc_status) noexcept { ResumeLater(wc_status); }

  ibv_send_wr* wr() { return &work_request_.wr; }
  bool is_read() const { return work_request_.wr.opcode == IBV_WR_RDMA_READ; }

  OpAwaiter* park_next = nullptr;

 private:
  SendQueue* queue_;
  SendWorkRequest work_request_;
};

static_assert(alignof(OpAwaiter) > kWrTagMask,
              "wr_id steals the low bits of the awaiter address");

class BatchAwaiter final : public IoAwaiter<BatchAwaiter> {
 public:
  BatchAwaiter(SendQueue* queue, ibv_send_wr* first_wr, ibv_send_wr* last_wr,
               uint32_t num_wrs, uint32_t num_read_wrs) noexcept
      : queue_(queue),
        first_wr_(first_wr),
        last_wr_(last_wr),
        num_wrs_(num_wrs),
        num_read_wrs_(num_read_wrs) {}

  bool await_ready() const noexcept { return num_wrs_ == 0; }

  Status await_resume() const {
    if (num_wrs_ == 0) {
      return Status::OK();
    }
    return ToStatus(static_cast<ibv_wc_status>(result_), "complete rdma batch");
  }

  void Arm() {
    last_wr_->send_flags |= IBV_SEND_SIGNALED;
    last_wr_->wr_id = MakeWrId(this, kTagBatchEnd);
    queue_->SubmitBatch(this);
  }

  void OnComplete(int32_t wc_status) noexcept {
    queue_->Release(num_wrs_, num_read_wrs_);
    ResumeLater(wc_status);
  }

  void Abort(int32_t wc_status) noexcept { ResumeLater(wc_status); }

  ibv_send_wr* first_wr() const { return first_wr_; }
  ibv_send_wr* last_wr() const { return last_wr_; }
  uint32_t num_wrs() const { return num_wrs_; }
  uint32_t num_read_wrs() const { return num_read_wrs_; }

 private:
  SendQueue* queue_;
  ibv_send_wr* first_wr_;
  ibv_send_wr* last_wr_;
  uint32_t num_wrs_;
  uint32_t num_read_wrs_;
};

static_assert(alignof(BatchAwaiter) > kWrTagMask,
              "wr_id steals the low bits of the awaiter address");

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_SEND_QUEUE_H_
