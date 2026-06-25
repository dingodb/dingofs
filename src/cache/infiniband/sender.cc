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
 * Created Date: 2026-06-13
 * Author: Jingli Chen (Wine93)
 */

#include "cache/infiniband/sender.h"

#include <gflags/gflags.h>

#include "cache/infiniband/common.h"
#include "cache/infiniband/connection.h"
#include "common/io_buffer.h"

namespace dingofs {
namespace cache {
namespace infiniband {

DEFINE_uint32(rdma_client_signal_request_send_every, 1024,
              "signal one client request send every n requests; 0 disables "
              "periodic signaled request sends");

RequestSender::RequestSender(Connection* conn) : conn_(conn), seq_num_(0) {}

bool RequestSender::ShouldSignal() {
  auto seq_num = seq_num_.fetch_add(1, std::memory_order_relaxed);
  bool signal_every = FLAGS_rdma_client_signal_request_send_every;
  if (signal_every != 0 && seq_num % signal_every == 0) {
    return true;
  }
  return false;
}

Status RequestSender::Send(RDMABuffer* request) {
  SendWorkRequest wr;
  InflightContext ctx(1);
  wr.opcode = OpCode::kSend;
  wr.addr = reinterpret_cast<uint64_t>(request->data);
  wr.length = request->length;
  wr.lkey = request->lkey;
  wr.signaled = true;
  wr.ctx = ctx.contexts.data();
  wr.ctx->on_completion = [&ctx](const WorkCompletion& wc) {
    ctx.status = wc.status;
    ctx.inflights.signal(1);
  };

  auto status = conn_->PostSendWorkRequest(wr);
  if (!status.ok()) {
    return status;
  }

  ctx.inflights.wait();
  return ctx.status;
}

ResponseSender::ResponseSender(Connection* conn) : conn_(conn) {}

Status ResponseSender::Send(RDMABuffer* response,
                            const Attachment& attachment) {
  std::vector<SendWorkRequest> work_requests;

  // part1: attachment
  if (attachment.buffer.Size() > 0) {
    SendWorkRequest wr;
    auto status = PrepWorkRequest(attachment, &wr);
    if (!status.ok()) {
      return status;
    }
    work_requests.push_back(wr);
  }

  // part2: response
  InflightContext ctx(1);
  {
    SendWorkRequest wr;
    PrepWorkRequest(&ctx, response, &wr);
    work_requests.push_back(wr);
  }

  auto status = conn_->PostSendWorkRequests(work_requests);
  if (!status.ok()) {
    return status;
  }

  ctx.inflights.wait();
  return ctx.status;
}

Status ResponseSender::CheckAttachment(const IOBuffer& src,
                                       const Region& dest) {
  if (src.ConstIOBuf().backing_block_num() != 1) {
    return Status::InvalidParam("buffer is not continuous");
  } else if (src.GetFirstDataMeta() == 0) {
    return Status::InvalidParam("buffer not register for rdma");
  } else if (src.Length() > dest.length) {
    return Status::InvalidParam(
        "response attachment exceeds advertised rdma length");
  } else if (dest.addr == 0 || dest.rkey == 0) {
    return Status::Internal("response rdma memory context is missing");
  }
  return Status::OK();
}

Status ResponseSender::PrepWorkRequest(const Attachment& attachment,
                                       SendWorkRequest* wr) {
  const auto& src = attachment.buffer;
  const auto& dest = attachment.dest;
  auto status = CheckAttachment(src, dest);
  if (!status.ok()) {
    return status;
  }

  wr->opcode = OpCode::kRDMAWrite;
  wr->addr = reinterpret_cast<uint64_t>(src.Fetch1());
  wr->length = static_cast<uint32_t>(src.Length());
  wr->lkey = src.GetFirstDataMeta();
  wr->raddr = dest.addr;
  wr->rkey = dest.rkey;
  wr->signaled = false;
  wr->ctx = nullptr;

  return Status::OK();
}

void ResponseSender::PrepWorkRequest(InflightContext* ctx, RDMABuffer* response,
                                     SendWorkRequest* wr) {
  wr->opcode = OpCode::kSend;
  wr->addr = reinterpret_cast<uint64_t>(response->data);
  wr->length = response->length;
  wr->lkey = response->lkey;
  wr->signaled = true;
  wr->ctx = ctx->contexts.data();
  wr->ctx->on_completion = [ctx](const WorkCompletion& wc) {
    ctx->status = wc.status;
    ctx->inflights.signal(1);
  };
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
