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
 * Created Date: 2026-04-27
 * Author: Jingli Chen (Wine93)
 */

#include "cache/infiniband/server_session.h"

#include <bthread/execution_queue.h>
#include <butil/iobuf.h>
#include <butil/memory/scope_guard.h>
#include <glog/logging.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "cache/common/slab_pool.h"
#include "cache/infiniband/common.h"
#include "cache/infiniband/connection.h"
#include "cache/infiniband/controller.h"
#include "cache/infiniband/memory.h"
#include "cache/infiniband/protocol.h"
#include "cache/infiniband/reader.h"
#include "cache/infiniband/sender.h"
#include "cache/infiniband/service.h"
#include "cache/iutil/bthread.h"
#include "common/io_buffer.h"
#include "common/status.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

using pb::infiniband::ErrorCode;

ServerSession::ServerSession(ConnectionUPtr conn, ServiceHub* service_hub)
    : conn_(std::move(conn)),
      recv_contexts_(conn_->GetRecvBufferPool()->BufferCount()),
      request_parser_(std::make_unique<RequestParser>(service_hub)),
      body_reader_(std::make_unique<BodyReader>(conn_.get())),
      response_serializer_(std::make_unique<ResponseSerializer>()),
      response_sender_(std::make_unique<ResponseSender>(conn_.get())) {
  keepalive_context_.on_completion = [this](const WorkCompletion&) {
    ReleaseKeepalive();
  };
}

ServerSession::~ServerSession() {
  DCHECK(closed_.load(std::memory_order_relaxed))
      << "ServerSession destroyed before Shutdown()";
}

Status ServerSession::Start() {
  bthread::ExecutionQueueOptions options;
  options.use_pthread = true;
  CHECK_EQ(0, bthread::execution_queue_start(&queue_id_, &options,
                                             HandleWorkCompletion, this))
      << "Fail to start ExecutionQueue for handle work completion";

  return OnEstablished();
}

void ServerSession::Shutdown() {
  if (closed_.exchange(true, std::memory_order_acq_rel)) {
    return;
  }

  broken_.store(true, std::memory_order_relaxed);
  auto status = conn_->GetQueuePair()->ModifyQpToError();
  LOG_IF(WARNING, !status.ok()) << "Fail to fence " << *conn_->GetQueuePair()
                                << ": " << status.ToString();

  gate_.Close();

  GetGlobalEventDispatcher(conn_->GetFd()).DelEvent(conn_->GetFd());

  CHECK_EQ(0, bthread::execution_queue_stop(queue_id_));
  CHECK_EQ(0, bthread::execution_queue_join(queue_id_));

  LOG(INFO) << "Shutdown server session: " << *conn_->GetQueuePair();
}

void ServerSession::SendKeepalive() {
  if (IsBroken() ||
      keepalive_inflight_.exchange(true, std::memory_order_acq_rel)) {
    return;
  }

  keepalive_buffer_ = conn_->GetSendBufferPool()->Alloc();
  if (keepalive_buffer_ == nullptr) {
    keepalive_inflight_.store(false, std::memory_order_release);
    return;
  }

  pb::infiniband::ResponseMeta meta;
  auto status =
      Protocol::SerializeResponse(0, meta, nullptr, keepalive_buffer_);
  if (status.ok()) {
    SendWorkRequest wr;
    wr.opcode = OpCode::kSend;
    wr.addr = reinterpret_cast<uint64_t>(keepalive_buffer_->data);
    wr.length = keepalive_buffer_->length;
    wr.lkey = keepalive_buffer_->lkey;
    wr.signaled = true;
    wr.ctx = &keepalive_context_;
    status = conn_->PostSendWorkRequest(wr);
  }

  if (!status.ok()) {
    ReleaseKeepalive();
    MarkBroken(status);
  }
}

void ServerSession::HandleEvent() {
  conn_->HandleCompletion([this](WorkCompletions cqes) {
    int rc = bthread::execution_queue_execute(queue_id_, cqes);
    LOG_IF(WARNING, rc != 0) << "Drop " << cqes.size()
                             << " work completions: execution queue stopped";
  });
}

int ServerSession::HandleWorkCompletion(
    void* meta, bthread::TaskIterator<WorkCompletions>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  auto* session = static_cast<ServerSession*>(meta);
  for (; iter; iter++) {
    for (const auto& wc : *iter) {
      if (!wc.status.ok()) {
        session->MarkBroken(wc.status);
      }

      auto* ctx = wc.ctx;
      if (ctx && ctx->on_completion) {
        ctx->on_completion(wc);
      }
    }
  }
  return 0;
}

void ServerSession::MarkBroken(const Status& status) {
  if (broken_.exchange(true, std::memory_order_relaxed)) {
    return;
  }
  LOG(WARNING) << "Server session is broken: " << *conn_->GetQueuePair()
               << " reason=" << status.ToString();
}

void ServerSession::ReleaseKeepalive() {
  conn_->GetSendBufferPool()->Free(keepalive_buffer_);
  keepalive_buffer_ = nullptr;
  keepalive_inflight_.store(false, std::memory_order_release);
}

Status ServerSession::OnEstablished() {
  std::vector<RecvWorkRequest> work_requets;
  auto* recv_buffer_pool = conn_->GetRecvBufferPool();

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

void ServerSession::PrepRecvWorkRequest(RDMABuffer* recv_buffer,
                                        RecvWorkRequest* wr) {
  wr->addr = reinterpret_cast<uint64_t>(recv_buffer->data);
  wr->length = recv_buffer->capacity;
  wr->lkey = recv_buffer->lkey;
  wr->ctx = &recv_contexts_[recv_buffer->index];
  wr->ctx->on_completion = [this, recv_buffer](const WorkCompletion& wc) {
    OnNewMessage(wc, recv_buffer);
  };
}

void ServerSession::OnNewMessage(const WorkCompletion& wc,
                                 RDMABuffer* recv_buffer) {
  if (!wc.status.ok() || IsBroken() || !gate_.Enter()) {
    return;
  }

  recv_buffer->length = wc.byte_len;
  bool started = iutil::StartBthread([this, recv_buffer]() {
    HandleNewMessage(recv_buffer);
    gate_.Leave();
  });
  if (!started) {
    gate_.Leave();
    MarkBroken(Status::Internal("start request handler failed"));
  }
}

void ServerSession::HandleNewMessage(RDMABuffer* buffer) {
  Controller cntl;

  RequestParser::Result result;
  ParseRequest(&cntl, buffer, &result);
  if (cntl.Failed()) {
    SendResponse(&cntl, result.correlation_id);
    return;
  }

  const auto& request_meta = result.request_meta;
  if (request_meta.attachment_size() > 0) {
    auto regions = FromPbRegions(request_meta.read_regions());
    size_t size = request_meta.attachment_size();
    ReadAttachment(&cntl, regions, size);
    if (cntl.Failed()) {
      LOG(ERROR) << "Fail to read request attachment: method="
                 << request_meta.method_name()
                 << " correlation_id=" << result.correlation_id
                 << " error=" << cntl.ErrorText();
      SendResponse(&cntl, result.correlation_id);
      return;
    }
  }

  ProcessRequest(result.service, result.method, &cntl, result.request,
                 result.response);

  Attachment attachment;
  if (!cntl.Failed()) {
    attachment.buffer = cntl.response_attachment();
    attachment.dest = request_meta.write_region();
    auto status =
        ResponseSender::CheckAttachment(attachment.buffer, attachment.dest);
    if (!status.ok()) {
      LOG(ERROR) << "Invalid response attachment: method="
                 << request_meta.method_name()
                 << " correlation_id=" << result.correlation_id
                 << " status=" << status.ToString();
      SetFailed(&cntl, ErrorCode::ProtocolError, status.ToString());
      attachment = Attachment();
    }
  }
  SendResponse(&cntl, result.correlation_id, result.response, attachment);
}

void ServerSession::ParseRequest(Controller* cntl, RDMABuffer* buffer,
                                 RequestParser::Result* result) {
  BRPC_SCOPE_EXIT {
    RecvWorkRequest wr;
    PrepRecvWorkRequest(buffer, &wr);
    auto status = conn_->PostRecvWorkRequest(wr);
    if (!status.ok()) {
      SetFailed(cntl, ErrorCode::QueuePairError, status.ToString());
    }
  };

  auto status = request_parser_->Parse(buffer, result, {cntl->Arena()});
  if (!status.ok()) {
    SetFailed(cntl, ErrorCode::ProtocolError, status.ToString());
  }
}

void ServerSession::ReadAttachment(Controller* cntl,
                                   const std::vector<Region>& src,
                                   size_t size) {
  auto status = BodyReader::CheckSource(src, size);
  if (!status.ok()) {
    SetFailed(cntl, ErrorCode::ProtocolError, status.ToString());
    return;
  }

  auto lease = GetGlobalSlabPool()->Acquire(size);
  if (!lease.ok()) {
    SetFailed(cntl, ErrorCode::NoMem, "alloc request attachment buffer failed");
    return;
  } else if (lease.lkey() == 0) {
    SetFailed(cntl, ErrorCode::InternalError,
              "request attachment buffer is not registered for rdma");
    return;
  }

  status = body_reader_->Read(lease.data(), lease.lkey(), src, size);
  if (!status.ok()) {
    MarkBroken(status);
    SetFailed(cntl, ErrorCode::InternalError, status.ToString());
    return;
  }

  IOBuffer attachment;
  lease.MoveInto(&attachment, size);
  cntl->request_attachment() = std::move(attachment);
}

struct BlockingClosure : public ::google::protobuf::Closure {
  void Wait() { inflight.wait(); }
  void Run() override { inflight.signal(); }
  bthread::CountdownEvent inflight{1};
};

void ServerSession::ProcessRequest(
    google::protobuf::Service* service,
    const google::protobuf::MethodDescriptor* method,
    google::protobuf::RpcController* controller,
    const google::protobuf::Message* request,
    google::protobuf::Message* response) {
  BlockingClosure done;
  service->CallMethod(method, controller, request, response, &done);
  done.Wait();
}

void ServerSession::SendResponse(Controller* cntl, uint64_t correlation_id,
                                 google::protobuf::Message* response,
                                 const Attachment& attachment) {
  auto* send_buffer_pool = conn_->GetSendBufferPool();
  auto* send_buffer = send_buffer_pool->Alloc();
  if (send_buffer == nullptr) {
    LOG(ERROR) << "Fail to send response because send buffer is exhausted: "
                  "correlation_id="
               << correlation_id;
    return;
  }

  BRPC_SCOPE_EXIT { send_buffer_pool->Free(send_buffer); };

  ResponseSerializer::Context ctx;
  ctx.correlation_id = correlation_id;
  ctx.error_code = cntl->ErrorCode();
  ctx.error_message = cntl->ErrorText();
  ctx.attachment_size = attachment.buffer.Size();
  ctx.response = response;
  auto status = response_serializer_->Serialize(ctx, send_buffer);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to serialize response: correlation_id="
               << correlation_id << " status=" << status.ToString();
    return;
  }

  status = response_sender_->Send(send_buffer, attachment);
  if (!status.ok()) {
    MarkBroken(status);
  }
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
