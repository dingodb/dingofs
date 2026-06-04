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

#include "cache/common/closure.h"
#include "cache/infiniband/connection.h"
#include "cache/infiniband/memory.h"
#include "cache/infiniband/protocol.h"
#include "common/status.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

DEFINE_int32(rdma_rpc_timeout_ms, 3000,
             "Timeout for an RDMA RPC response in milliseconds");
DEFINE_uint32(rdma_client_signal_request_send_every, 1024,
              "Signal one client request SEND every N requests so "
              "high-concurrency writers do not overrun the QP send queue; "
              "0 disables signaled request SENDs");

using pb::infiniband::ErrorCode;

ClientSession::ClientSession(ConnectionUPtr conn)
    : conn_(std::move(conn)),
      recv_wr_context_(conn_->GetRecvBufferPool()->BufferCount()),
      handle_wc_queue_id_({0}) {}

void ClientSession::Start() {
  bthread::ExecutionQueueOptions options;
  options.use_pthread = true;
  CHECK_EQ(0, bthread::execution_queue_start(&handle_wc_queue_id_, &options,
                                             HandleWorkCompletion, this))
      << "Fail to start ExecutionQueue for handle work completion";
}

void ClientSession::Shutdown() {
  // 1. Stop the event dispatcher from delivering new completions to this
  //    session, so no HandleEvent() starts after we drain below. A HandleEvent()
  //    already in flight keeps this session alive via the worker's strong ref.
  GetGlobalEventDispatcher(conn_->GetFd()).DelEvent(conn_->GetFd());

  // 2. Drain the completion execution queue: after join no OnResponseReceived
  //    runs, so no Controller is touched concurrently after this point.
  CHECK_EQ(0, bthread::execution_queue_stop(handle_wc_queue_id_));
  CHECK_EQ(0, bthread::execution_queue_join(handle_wc_queue_id_));

  // 3. Fail any still-outstanding waiters so callers return immediately instead
  //    of blocking to the full RPC timeout. Their Controllers are alive (the
  //    calling bthread is still parked in DoCall's wait until we signal).
  std::lock_guard<std::mutex> guard(outstanding_mutex_);
  for (auto& [id, cntl] : outstanding_) {
    cntl->SetErrorCode(ErrorCode::InternalError);
    cntl->SetFailed("rdma session shutdown");
    cntl->response_received().signal();
  }
  outstanding_.clear();
}

void ClientSession::HandleEvent() {
  conn_->HandleCompletion([this](WorkCompletions wcs) {
    // The queue may already be stopped if a teardown raced this event; drop the
    // completions in that case instead of CHECK-failing.
    int rc = bthread::execution_queue_execute(handle_wc_queue_id_, wcs);
    LOG_IF(WARNING, rc != 0)
        << "Drop " << wcs.size() << " work completions: execution queue stopped";
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
      if (ctx->on_completion) {
        ctx->on_completion(wc);
      }
    }
  }

  return 0;
}

void ClientSession::PrepRecvWorkRequest(RdmaBuffer* recv_buffer,
                                        RecvWorkRequest* wr) {
  wr->addr = reinterpret_cast<uint64_t>(recv_buffer->data);
  wr->length = recv_buffer->capacity;
  wr->lkey = recv_buffer->lkey;
  wr->ctx = &recv_wr_context_[recv_buffer->index];
  wr->ctx->on_completion = [this, recv_buffer](const WorkCompletion& wc) {
    OnResponseReceived(wc, recv_buffer);
  };
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
    LOG(ERROR) << "Fail to post receive work requests";
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
  cntl->Reset();
  cntl->response_received().reset(1);
  // Monotonic correlation id (never the Controller pointer): a late response
  // can't be matched to an unrelated request that happens to reuse the address.
  const uint64_t correlation_id =
      next_correlation_id_.fetch_add(1, std::memory_order_relaxed);
  cntl->correlation_id() = correlation_id;
  cntl->response() = response;

  auto* send_buffer_pool = conn_->GetSendBufferPool();
  auto* send_buffer = send_buffer_pool->Alloc();
  if (send_buffer == nullptr) {
    LOG(ERROR) << "Fail to send request because send buffer is exhausted";
    OnError(cntl, ErrorCode::NoMem, "alloc send buffer failed");
    return;
  }

  // Keep the in-band request buffer reserved until the server response arrives.
  // A response proves the peer has consumed the SEND, so we can avoid one
  // signaled SEND completion and one per-RPC wait on the hot Range/Cache path.
  BRPC_SCOPE_EXIT { send_buffer_pool->Free(send_buffer); };

  // Register as outstanding before posting so a fast response finds the waiter.
  {
    std::lock_guard<std::mutex> guard(outstanding_mutex_);
    outstanding_[correlation_id] = cntl;
  }

  SendRequest(cntl, service_name, method_name, request, send_buffer);
  if (cntl->Failed()) {
    std::lock_guard<std::mutex> guard(outstanding_mutex_);
    outstanding_.erase(correlation_id);
    return;
  }

  int rc = 0;
  if (FLAGS_rdma_rpc_timeout_ms <= 0) {
    rc = cntl->response_received().wait();
  } else {
    rc = cntl->response_received().timed_wait(
        butil::milliseconds_from_now(FLAGS_rdma_rpc_timeout_ms));
  }
  if (rc == 0) {
    // OnResponseReceived (or Shutdown) already removed the entry and populated
    // / failed the Controller before signalling. Nothing left to do.
    return;
  }

  // Timed out or wait failed: remove the waiter so a late response is dropped.
  // Taking the lock also serialises with an in-flight OnResponseReceived, which
  // finishes touching the Controller before releasing the lock -- so it is safe
  // to let the (stack) Controller go out of scope once we return.
  {
    std::lock_guard<std::mutex> guard(outstanding_mutex_);
    outstanding_.erase(correlation_id);
  }
  if (rc == ETIMEDOUT) {
    OnError(cntl, ErrorCode::Timeout, "wait response timeout");
  } else {
    OnError(cntl, ErrorCode::InternalError, "wait response failed");
  }
}

void ClientSession::SendRequest(Controller* cntl, std::string_view service_name,
                                std::string_view method_name,
                                const google::protobuf::Message& request,
                                RdmaBuffer* send_buffer) {
  auto& request_meta = cntl->request_meta();
  *request_meta.mutable_service_name() = service_name;
  *request_meta.mutable_method_name() = method_name;
  const auto& attachment = cntl->request_attachment();
  if (attachment.Size() > 0) {
    request_meta.set_attachment_size(attachment.Size());
    // TODO: addr
  }

  auto status = Protocol::SerializeRequest(cntl->correlation_id(), request_meta,
                                           request, send_buffer);
  if (!status.ok()) {
    OnError(cntl, ErrorCode::ProtocolError, status.ToString());
    return;
  }

  SendWorkRequest wr;
  wr.addr = reinterpret_cast<uint64_t>(send_buffer->data);
  wr.length = send_buffer->length;
  wr.lkey = send_buffer->lkey;
  wr.opcode = OpCode::kSend;
  const uint32_t signal_every = FLAGS_rdma_client_signal_request_send_every;
  wr.signaled =
      signal_every != 0 &&
      (request_send_seq_.fetch_add(1, std::memory_order_relaxed) %
       signal_every) == 0;
  wr.ctx = &unsignaled_send_wr_context_;

  status = conn_->PostSendWorkRequest(wr);
  if (!status.ok()) {
    OnError(cntl, ErrorCode::QueuePairError, status.ToString());
    return;
  }
}

void ClientSession::OnResponseReceived(const WorkCompletion& wc,
                                       RdmaBuffer* buffer) {
  // Always return the recv buffer to the RQ when we are done with it, even if
  // the response is dropped below -- otherwise the receive queue drains.
  BRPC_SCOPE_EXIT {
    RecvWorkRequest work_request;
    PrepRecvWorkRequest(buffer, &work_request);
    auto status = conn_->PostRecvWorkRequest(work_request);
    if (!status.ok()) {
      LOG(ERROR) << "Fail to re-post recv work request: " << status.ToString();
    }
  };

  if (!wc.status.ok()) {
    LOG(ERROR) << "Fail to execute receive work request: "
               << wc.status.ToString();
    return;
  }

  buffer->length = wc.byte_len;
  uint64_t correlation_id;
  auto status = Protocol::PeekCorrelationId(buffer, &correlation_id);
  if (!status.ok()) {
    LOG(ERROR) << "Receive invalid response which missing correlation_id";
    return;
  }

  // Resolve the waiting Controller under the lock. If it is gone the request
  // already timed out (and its stack Controller was freed); drop the late
  // response rather than touching freed memory. Holding the lock across
  // ParseResponse + signal serialises with DoCall's timeout path.
  std::lock_guard<std::mutex> guard(outstanding_mutex_);
  auto it = outstanding_.find(correlation_id);
  if (it == outstanding_.end()) {
    return;
  }
  auto* cntl = it->second;
  outstanding_.erase(it);
  ParseResponse(cntl, buffer);
  cntl->response_received().signal();
}

void ClientSession::ParseResponse(Controller* cntl, RdmaBuffer* buffer) {
  uint64_t correlation_id;
  pb::infiniband::ResponseMeta response_meta;
  std::string_view response_view;
  auto status = Protocol::ParseResponse(buffer, &correlation_id, &response_meta,
                                        &response_view);
  if (!status.ok()) {
    OnError(cntl, ErrorCode::ProtocolError, status.ToString());
    return;
  }

  cntl->correlation_id() = correlation_id;
  cntl->response_meta() = response_meta;
  cntl->SetErrorCode(response_meta.error_code());
  if (response_meta.error_code() != ErrorCode::Ok) {
    cntl->SetFailed(response_meta.error_message().empty()
                        ? "remote rdma request failed"
                        : response_meta.error_message());
  }
  if (!cntl->Failed() && !cntl->response()->ParseFromArray(
                             response_view.data(), response_view.size())) {
    OnError(cntl, ErrorCode::ProtocolError, "parse response failed");
  }
}

void ClientSession::OnError(Controller* cntl,
                            pb::infiniband::ErrorCode error_code,
                            const std::string& reason) {
  cntl->SetErrorCode(error_code);
  cntl->SetFailed(reason);
  LOG(ERROR) << "Fail to process request: " << reason;
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
