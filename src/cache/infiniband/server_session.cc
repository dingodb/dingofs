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
      response_sender_(std::make_unique<ResponseSender>(conn_.get())),
      joiner_(std::make_unique<iutil::BthreadJoiner>()) {}

Status ServerSession::Start() {
  bthread::ExecutionQueueOptions options;
  options.use_pthread = true;
  CHECK_EQ(0, bthread::execution_queue_start(&queue_id_, &options,
                                             HandleWorkCompletion, this))
      << "Fail to start ExecutionQueue for handle work completion";

  joiner_->Start();

  return OnEstablished();
}

void ServerSession::Shutdown() {
  GetGlobalEventDispatcher(conn_->GetFd()).DelEvent(conn_->GetFd());

  CHECK_EQ(0, bthread::execution_queue_stop(queue_id_));
  CHECK_EQ(0, bthread::execution_queue_join(queue_id_));

  joiner_->Shutdown();
}

void ServerSession::HandleEvent() {
  conn_->HandleCompletion([this](WorkCompletions cqes) {
    // TODO: close graceful
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
      auto* ctx = wc.ctx;
      if (ctx && ctx->on_completion) {
        ctx->on_completion(wc);
      }
    }
  }
  return 0;
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
  if (!wc.status.ok()) {
    LOG(ERROR) << "Fail to execute receive work request: "
               << wc.status.ToString();
    return;
  }

  recv_buffer->length = wc.byte_len;
  auto tid = iutil::RunInBthread(
      [this, recv_buffer]() { HandleNewMessage(recv_buffer); });
  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
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
  // The lease returns the slab on every early-return path; only MoveInto (the
  // success path) hands it to the IOBuffer's refcount world.
  auto lease = GetGlobalSlabPool()->Acquire(size);
  if (!lease.ok()) {
    SetFailed(cntl, ErrorCode::NoMem, "alloc request attachment buffer failed");
    return;
  } else if (lease.lkey() == 0) {
    SetFailed(cntl, ErrorCode::InternalError,
              "request attachment buffer is not registered for rdma");
    return;
  }

  auto status = body_reader_->Read(lease.data(), lease.lkey(), src, size);
  if (!status.ok()) {
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
    LOG(ERROR) << "Fail to send response because send buffer is exhausted";
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
    LOG(ERROR) << "Fail to send response for serialize response failed";
    return;
  }

  status = response_sender_->Send(send_buffer, attachment);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to send response: " << status.ToString();
  }
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
