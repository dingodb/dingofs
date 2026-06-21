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

/*
 * Project: DingoFS
 * Created Date: 2026-05-07
 * Author: Jingli Chen (Wine93)
 */

#include "cache/infiniband/connection.h"

#include <glog/logging.h>
#include <infiniband/verbs.h>

#include <cstdint>

#include "cache/infiniband/infiniband.h"
#include "cache/infiniband/memory.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace infiniband {

DEFINE_int32(rdma_send_buffer_size, 4096, "Size of each send buffer in bytes");
DEFINE_int32(rdma_send_queue_size, 4096,
             "Maxinum number of send work requests to post to send queue");
DEFINE_int32(rdma_recv_buffer_size, 4096,
             "Size of each receive buffer in bytes");
DEFINE_int32(
    rdma_recv_queue_size, 4096,
    "Maxinum number of receiver work requests to post to receive queue");

Connection::Connection(QueuePairUPtr queue_pair,
                       CompletionQueueUPtr completion_queue)
    : send_buffer_pool_(RDMABufferPool::Create(queue_pair->GetProtectDomain(),
                                               FLAGS_rdma_send_buffer_size,
                                               FLAGS_rdma_send_queue_size)),
      recv_buffer_pool_(RDMABufferPool::Create(queue_pair->GetProtectDomain(),
                                               FLAGS_rdma_recv_buffer_size,
                                               FLAGS_rdma_recv_queue_size)),
      queue_pair_(std::move(queue_pair)),
      completion_queue_(std::move(completion_queue)) {}

Status Connection::PostSendWorkRequests(
    const std::vector<SendWorkRequest>& entries) {
  int wr_num = entries.size();
  std::vector<ibv_send_wr> work_requests(wr_num);
  std::vector<ibv_sge> sges(wr_num);

  for (int i = 0; i < wr_num; i++) {
    if (entries[i].signaled && entries[i].ctx == nullptr) {
      LOG(ERROR) << "Signaled send work request is missing completion context";
      return Status::InvalidParam(
          "signaled send work request missing completion context");
    }

    // wr_id
    work_requests[i].wr_id = reinterpret_cast<uint64_t>(entries[i].ctx);

    // next
    work_requests[i].next = nullptr;
    if (i > 0) {
      work_requests[i - 1].next = &work_requests[i];
    }

    // sg_list
    sges[i].addr = entries[i].addr;
    sges[i].length = entries[i].length;
    sges[i].lkey = entries[i].lkey;
    work_requests[i].sg_list = &sges[i];
    work_requests[i].num_sge = 1;

    // opcode
    if (entries[i].opcode == OpCode::kSend) {
      work_requests[i].opcode = IBV_WR_SEND;
    } else if (entries[i].opcode == OpCode::kRDMAWrite) {
      work_requests[i].opcode = IBV_WR_RDMA_WRITE;
    } else if (entries[i].opcode == OpCode::kRDMARead) {
      work_requests[i].opcode = IBV_WR_RDMA_READ;
    } else {
      CHECK(false) << "Unsupport send optype";
    }

    // send_flags
    if (entries[i].signaled) {
      work_requests[i].send_flags = IBV_SEND_SIGNALED;
    }

    // wr
    if (entries[i].opcode == OpCode::kRDMAWrite ||
        entries[i].opcode == OpCode::kRDMARead) {
      work_requests[i].wr.rdma.remote_addr = entries[i].raddr;
      work_requests[i].wr.rdma.rkey = entries[i].rkey;
    }
  }

  // TODO: modify qp to error if error
  ibv_send_wr* bad_work_request = nullptr;
  int rc = ibv_post_send(queue_pair_->GetIbQp(), work_requests.data(),
                         &bad_work_request);
  if (rc != 0 || bad_work_request != nullptr) {
    PLOG(ERROR) << "Fail to post send work requests";
    return Status::Internal("post send work requests failed");
  }
  return Status::OK();
}

Status Connection::PostRecvWorkRequests(
    const std::vector<RecvWorkRequest>& entries) {
  int wr_num = entries.size();
  std::vector<ibv_recv_wr> work_requests(wr_num);
  std::vector<ibv_sge> sges(wr_num);

  for (int i = 0; i < wr_num; i++) {
    CHECK_NOTNULL(entries[i].ctx);

    // wr_id
    work_requests[i].wr_id = reinterpret_cast<uint64_t>(entries[i].ctx);

    // next
    work_requests[i].next = nullptr;
    if (i != 0) {
      work_requests[i - 1].next = &work_requests[i];
    }

    // sg_list
    sges[i].addr = entries[i].addr;
    sges[i].length = entries[i].length;
    sges[i].lkey = entries[i].lkey;
    work_requests[i].sg_list = &sges[i];
    work_requests[i].num_sge = 1;
  }

  // TODO: modify qp to error if error
  ibv_recv_wr* bad_work_request = nullptr;
  int rc = ibv_post_recv(queue_pair_->GetIbQp(), work_requests.data(),
                         &bad_work_request);
  if (rc != 0 || bad_work_request != nullptr) {
    PLOG(ERROR) << "Fail to post receive work requests";
    return Status::Internal("post receive work requests failed");
  }
  return Status::OK();
}

void Connection::HandleCompletion(CompletionHandler handler) {
  // TODO: modify qp state to error
  CHECK(completion_queue_->GetCqEvent());
  CHECK(PollCompletionQueue(handler));
  CHECK(completion_queue_->RearmNotify());
  CHECK(PollCompletionQueue(handler));
}

bool Connection::PollCompletionQueue(CompletionHandler handler) {
  static constexpr int kMaxWcNum = 32;
  ibv_wc cqe[kMaxWcNum];
  auto* cq = completion_queue_->GetIbCq();

  do {
    int n = ibv_poll_cq(cq, kMaxWcNum, cqe);
    if (n < 0) {
      PLOG(ERROR) << "Fail to poll completion queue";
      return false;
    } else if (n == 0) {
      return true;
    }

    auto wcs = std::vector<WorkCompletion>(n);
    for (int i = 0; i < n; i++) {
      auto& wc = wcs[i];

      // wr_id
      wc.ctx = reinterpret_cast<WorkRequestContext*>(cqe[i].wr_id);

      // status
      switch (cqe[i].status) {
        case IBV_WC_SUCCESS:
          wc.status = Status::OK();
          break;
        default:
          wc.status = Status::Internal(ibv_wc_status_str(cqe[i].status));
          LOG(ERROR) << "[DBG] WR failed: wr_id=0x" << std::hex << cqe[i].wr_id
                     << " opcode=" << std::dec << cqe[i].opcode
                     << " vendor_err=0x" << std::hex << cqe[i].vendor_err
                     << " status=" << ibv_wc_status_str(cqe[i].status);
      }

      // opcode
      switch (cqe[i].opcode) {
        case IBV_WC_SEND:
          wc.opcode = OpCode::kSend;
          break;
        case IBV_WC_RECV:
          wc.opcode = OpCode::kRecv;
          break;
        case IBV_WC_RDMA_WRITE:
          wc.opcode = OpCode::kRDMAWrite;
          break;
        case IBV_WC_RDMA_READ:
          wc.opcode = OpCode::kRDMARead;
          break;
        default:
          wc.opcode = OpCode::kUnknown;
      }

      // bytes_len
      wc.byte_len = cqe[i].byte_len;
    }

    handler(std::move(wcs));

    if (n < kMaxWcNum) {
      break;
    }
  } while (true);

  return true;
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
