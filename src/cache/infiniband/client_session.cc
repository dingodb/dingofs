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
 * Created Date: 2026-04-29
 * Author: Jingli Chen (Wine93)
 */

#include "cache/infiniband/client_session.h"

#include <bthread/execution_queue.h>
#include <butil/memory/scope_guard.h>
#include <butil/time.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <cerrno>
#include <cstdint>
#include <utility>
#include <vector>

#include "cache/infiniband/common.h"
#include "cache/infiniband/connection.h"
#include "cache/infiniband/memory.h"
#include "cache/infiniband/protocol.h"
#include "cache/infiniband/waiter.h"
#include "common/status.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

DEFINE_int32(rdma_rpc_timeout_ms, 3000,
             "timeout for an rdma rpc response in milliseconds");

using pb::infiniband::ErrorCode;

ClientSession::ClientSession(ConnectionUPtr conn)
    : conn_(std::move(conn)),
      next_correlation_id_(1),
      recv_contexts_(conn_->GetRecvBufferPool()->BufferCount()),
      request_serializer_(std::make_unique<RequestSerializer>()),
      request_sender_(std::make_unique<RequestSender>(conn_.get())),
      response_parser_(std::make_unique<ResponseParser>()),
      waiters_(std::make_unique<Waiters>()) {}

Status ClientSession::Start() {
  bthread::ExecutionQueueOptions options;
  options.use_pthread = true;
  CHECK_EQ(0, bthread::execution_queue_start(&queue_id_, &options,
                                             HandleWorkCompletion, this))
      << "Fail to start ExecutionQueue for handle work completion";

  return OnEstablished();
}

void ClientSession::Shutdown() {
  GetGlobalEventDispatcher(conn_->GetFd()).DelEvent(conn_->GetFd());

  CHECK_EQ(0, bthread::execution_queue_stop(queue_id_));
  CHECK_EQ(0, bthread::execution_queue_join(queue_id_));

  std::vector<Waiter*> waiters;
  waiters_->GetAll(&waiters);
  for (auto* waiter : waiters) {
    SetFailed(waiter->ctx.cntl, ErrorCode::InternalError,
              "rdma session shutdown");
    waiter->notify.signal();
  }
}

void ClientSession::HandleEvent() {
  conn_->HandleCompletion([this](WorkCompletions cqes) {
    int rc = bthread::execution_queue_execute(queue_id_, cqes);
    // TODO: close graceful
    LOG_IF(ERROR, rc != 0) << "Drop " << cqes.size()
                           << " work completions: execution queue stopped";
  });
}

int ClientSession::HandleWorkCompletion(
    void* meta, bthread::TaskIterator<WorkCompletions>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  auto* session = static_cast<ClientSession*>(meta);
  for (; iter; iter++) {
    for (const auto& wc : *iter) {
      auto* ctx = wc.ctx;
      if (ctx && ctx->on_completion) {
        ctx->on_completion(wc);
      }
    }
  }

  return 0;
}

Status ClientSession::OnEstablished() {
  auto* recv_buffer_pool = conn_->GetRecvBufferPool();
  std::vector<RecvWorkRequest> work_requets;

  do {
    auto* recv_buffer = recv_buffer_pool->Alloc();
    if (nullptr == recv_buffer) {
      break;
    }

    RecvWorkRequest work_request;
    PrepRecvWorkRequest(recv_buffer, &work_request);
    work_requets.emplace_back(work_request);
  } while (true);

  auto status = conn_->PostRecvWorkRequests(work_requets);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to post receive work requests: " << status.ToString();
    return status;
  }

  LOG(INFO) << "Successfully post " << work_requets.size()
            << " receive work requests";
  return Status::OK();
}

void ClientSession::DoCall(Controller* cntl, std::string_view service_name,
                           std::string_view method_name,
                           const google::protobuf::Message& request,
                           google::protobuf::Message* response) {
  auto correlation_id = GetNextCorrelationId();

  Waiter waiter;
  waiter.correlation_id = correlation_id;
  waiter.ctx.cntl = cntl;
  waiter.ctx.response = response;
  waiters_->Add(correlation_id, &waiter);

  SendRequest(cntl, correlation_id, service_name, method_name, request);
  if (cntl->Failed()) {
    waiters_->Remove(correlation_id);
    return;
  }

  WaitingResponse(&waiter);
}

void ClientSession::SendRequest(Controller* cntl, uint64_t correlation_id,
                                std::string_view service_name,
                                std::string_view method_name,
                                const google::protobuf::Message& request) {
  auto* send_buffer_pool = conn_->GetSendBufferPool();
  auto* send_buffer = send_buffer_pool->Alloc();
  if (send_buffer == nullptr) {
    LOG(ERROR) << "Fail to send request because send buffer is exhausted";
    SetFailed(cntl, ErrorCode::NoMem, "alloc send buffer failed");
    return;
  }

  BRPC_SCOPE_EXIT { send_buffer_pool->Free(send_buffer); };

  RequestSerializer::Context ctx;
  ctx.correlation_id = correlation_id;
  ctx.service_name = service_name;
  ctx.method_name = method_name;
  ctx.attachment_size = cntl->request_attachment().Size();
  ctx.read_regions = FromPbRegions(cntl->read_regions());
  ctx.write_region = cntl->write_region();
  ctx.request = &request;
  auto status = request_serializer_->Serialize(ctx, send_buffer);
  if (!status.ok()) {
    SetFailed(cntl, ErrorCode::ProtocolError, status.ToString());
    return;
  }

  status = request_sender_->Send(send_buffer);
  if (!status.ok()) {
    SetFailed(cntl, ErrorCode::QueuePairError, status.ToString());
  }
}

void ClientSession::WaitingResponse(Waiter* waiter) {
  int rc = 0;
  int64_t timeout_ms = waiter->ctx.cntl->timeout_ms();
  if (timeout_ms < 0) {
    timeout_ms = FLAGS_rdma_rpc_timeout_ms;
  }

  if (timeout_ms <= 0) {
    rc = waiter->notify.wait();
  } else {
    rc = waiter->notify.timed_wait(butil::milliseconds_from_now(timeout_ms));
  }

  if (rc == 0) {  // response received
    return;
  }

  if (!waiters_->Remove(waiter->correlation_id)) {
    waiter->notify.wait();
    return;
  }

  auto* cntl = waiter->ctx.cntl;
  if (rc == ETIMEDOUT) {
    SetFailed(cntl, ErrorCode::Timeout, "wait response timeout");
  } else {
    SetFailed(cntl, ErrorCode::InternalError, "wait response failed");
  }
}

void ClientSession::PrepRecvWorkRequest(RDMABuffer* recv_buffer,
                                        RecvWorkRequest* wr) {
  wr->addr = reinterpret_cast<uint64_t>(recv_buffer->data);
  wr->length = recv_buffer->capacity;
  wr->lkey = recv_buffer->lkey;
  wr->ctx = &recv_contexts_[recv_buffer->index];
  wr->ctx->on_completion = [this, recv_buffer](const WorkCompletion& wc) {
    OnResponseReceived(wc, recv_buffer);
  };
}

void ClientSession::OnResponseReceived(const WorkCompletion& wc,
                                       RDMABuffer* recv_buffer) {
  BRPC_SCOPE_EXIT {
    RecvWorkRequest wr;
    PrepRecvWorkRequest(recv_buffer, &wr);
    auto status = conn_->PostRecvWorkRequest(wr);
    if (!status.ok()) {
      LOG(ERROR) << "Fail to re-post recv work request: " << status.ToString();
    }
  };

  if (!wc.status.ok()) {
    LOG(ERROR) << "Fail to execute receive work request: "
               << wc.status.ToString();
    return;
  }

  recv_buffer->length = wc.byte_len;
  uint64_t correlation_id;
  auto status = Protocol::PeekCorrelationId(recv_buffer, &correlation_id);
  if (!status.ok()) {
    LOG(ERROR) << "Receive invalid response which missing correlation_id";
    return;
  } else if (correlation_id == 0) {  // TODO: log error message
    LOG(ERROR) << "Receive response with 0 collrelation id";
    return;
  }

  auto* waiter = waiters_->Take(correlation_id);
  if (waiter == nullptr) {
    LOG(ERROR) << "Waiter{id=" << correlation_id << "} not found";
    return;
  }

  ParseResponse(waiter->ctx.cntl, recv_buffer, waiter->ctx.response);
  waiter->notify.signal();
}

void ClientSession::ParseResponse(Controller* cntl, RDMABuffer* recv_buffer,
                                  google::protobuf::Message* response) {
  ResponseParser::Result result;
  auto status = response_parser_->Parse(recv_buffer, &result, response);
  if (!status.ok()) {
    SetFailed(cntl, ErrorCode::ProtocolError, status.ToString());
    return;
  }

  cntl->SetErrorCode(result.error_code);
  if (result.error_code != ErrorCode::Ok) {
    cntl->SetFailed(result.error_message);
  }
  cntl->set_response_attachment_size(result.attachment_size);
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
