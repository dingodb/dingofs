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

#include "cache/infiniband/common.h"
#include "cache/infiniband/connection.h"
#include "common/io_buffer.h"

namespace dingofs {
namespace cache {
namespace infiniband {

DEFINE_uint32(rdma_client_signal_request_send_every, 1024,
              "Signal one client request SEND every N requests so "
              "high-concurrency writers do not overrun the QP send queue; "
              "0 disables signaled request SENDs");

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

Status ResponseSender::CheckRegion(const IOBuffer& buffer,
                                   const Region& region) {
  if (region.addr == 0 || region.rkey == 0) {
    return Status::Internal("response rdma memory context is missing");
  } else if (buffer.Length() > region.length) {
    return Status::Internal(
        "response attachment exceeds advertised rdma length");
  }
  return Status::OK();
}

Status ResponseSender::PrepWorkRequest(const Attachment& attachment,
                                       SendWorkRequest* wr) {
  const auto& buffer = attachment.buffer;
  const auto& region = attachment.dest;
  auto status = CheckRegion(buffer, region);
  if (!status.ok()) {
    return status;
  }

  wr->opcode = OpCode::kRDMAWrite;
  wr->addr = reinterpret_cast<uint64_t>(buffer.Fetch1());
  wr->length = static_cast<uint32_t>(buffer.Length());
  wr->lkey = buffer.GetFirstDataMeta();
  wr->raddr = region.addr;
  wr->rkey = region.rkey;
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
