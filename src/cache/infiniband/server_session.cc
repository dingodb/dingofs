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

#include "cache/common/closure.h"
#include "cache/common/slab_pool.h"
#include "cache/infiniband/connection.h"
#include "cache/infiniband/controller.h"
#include "cache/infiniband/protocol.h"
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
      service_hub_(service_hub),
      joiner_(std::make_unique<iutil::BthreadJoiner>()),
      recv_wr_context_(conn_->GetRecvBufferPool()->BufferCount()) {}

ServerSession::~ServerSession() = default;

void ServerSession::Start() {
  bthread::ExecutionQueueOptions options;
  options.use_pthread = true;
  CHECK_EQ(0, bthread::execution_queue_start(&handle_wc_queue_id_, &options,
                                             HandleWorkCompletion, this))
      << "Fail to start ExecutionQueue for handle work completion";

  joiner_->Start();
}

void ServerSession::Shutdown() {
  // Deregister from the event dispatcher first so no HandleEvent() starts after
  // we drain. An in-flight HandleEvent() keeps this session alive via the
  // worker's strong ref until it returns.
  GetGlobalEventDispatcher(conn_->GetFd()).DelEvent(conn_->GetFd());

  CHECK_EQ(0, bthread::execution_queue_stop(handle_wc_queue_id_));
  CHECK_EQ(0, bthread::execution_queue_join(handle_wc_queue_id_));

  joiner_->Shutdown();
}

void ServerSession::HandleEvent() {
  conn_->HandleCompletion([this](WorkCompletions cqe) {
    // The queue may already be stopped if a teardown raced this event; drop the
    // completions in that case instead of CHECK-failing.
    int rc = bthread::execution_queue_execute(handle_wc_queue_id_, cqe);
    LOG_IF(WARNING, rc != 0)
        << "Drop " << cqe.size() << " work completions: execution queue stopped";
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
      if (ctx->on_completion) {
        ctx->on_completion(wc);
      }
    }
  }
  return 0;
}

void ServerSession::PrepRecvWorkRequest(RdmaBuffer* recv_buffer,
                                        RecvWorkRequest* wr) {
  wr->addr = reinterpret_cast<uint64_t>(recv_buffer->data);
  wr->length = recv_buffer->capacity;
  wr->lkey = recv_buffer->lkey;
  wr->ctx = &recv_wr_context_[recv_buffer->index];
  wr->ctx->on_completion = [this, recv_buffer](const WorkCompletion& wc) {
    OnNewMessage(wc, recv_buffer);
  };
}

Status ServerSession::OnEstablished() {
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

void ServerSession::OnNewMessage(const WorkCompletion& wc, RdmaBuffer* buffer) {
  if (!wc.status.ok()) {
    LOG(ERROR) << "Fail to execute receive work request: "
               << wc.status.ToString();
    return;
  }

  buffer->length = wc.byte_len;
  auto tid =
      iutil::RunInBthread([this, buffer]() { HandleNewMessage(buffer); });
  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }
}

void ServerSession::HandleNewMessage(RdmaBuffer* buffer) {
  Controller cntl;

  ParseMessage(&cntl, buffer);
  if (cntl.Failed()) {
    SendResponse(&cntl);
    return;
  }

  if (cntl.request_attachment_size() > 0) {
    ReadRequestAttachment(&cntl);
    if (cntl.Failed()) {
      SendResponse(&cntl);
      return;
    }
  }

  ProcessRequest(&cntl);
  SendResponse(&cntl);
}

void ServerSession::ParseMessage(Controller* cntl, RdmaBuffer* buffer) {
  BRPC_SCOPE_EXIT {
    RecvWorkRequest work_request;
    PrepRecvWorkRequest(buffer, &work_request);
    auto status = conn_->PostRecvWorkRequest(work_request);
    if (!status.ok()) {
      OnError(cntl, ErrorCode::QueuePairError, status.ToString());
    }
  };

  uint64_t correlation_id;
  std::string_view request_view;
  auto status = Protocol::ParseRequest(buffer, &correlation_id,
                                       &cntl->request_meta(), &request_view);
  if (!status.ok()) {
    OnError(cntl, ErrorCode::ProtocolError, status.ToString());
    return;
  }

  google::protobuf::Service* service;
  status =
      service_hub_->GetService(cntl->request_meta().service_name(), &service);
  if (!status.ok()) {
    OnError(cntl, ErrorCode::ProtocolError, status.ToString());
    return;
  }

  google::protobuf::MethodDescriptor* method;
  status = service_hub_->GetMethod(service, cntl->request_meta().method_name(),
                                   &method);
  if (!status.ok()) {
    OnError(cntl, ErrorCode::ProtocolError, status.ToString());
    return;
  }

  auto* request = service->GetRequestPrototype(method).New(cntl->Arena());
  auto* response = service->GetResponsePrototype(method).New(cntl->Arena());
  if (!request->ParseFromArray(request_view.data(), request_view.size())) {
    OnError(cntl, ErrorCode::ProtocolError, "parse request failed");
  }

  cntl->correlation_id() = correlation_id;
  cntl->service() = service;
  cntl->method() = method;
  cntl->request() = request;
  cntl->response() = response;
}

void ServerSession::ReadRequestAttachment(Controller* cntl) {
  const size_t attachment_size = cntl->request_attachment_size();
  auto& slab_pool = dingofs::cache::GetGlobalRecvSlabPool();
  auto* buffer = slab_pool.Alloc(attachment_size);
  if (buffer == nullptr) {
    OnError(cntl, ErrorCode::NoMem, "alloc request attachment buffer failed");
    return;
  }
  if (buffer->meta == 0) {
    slab_pool.Free(buffer);
    OnError(cntl, ErrorCode::ProtocolError,
            "request attachment buffer is not registered for rdma");
    return;
  }

  // Collect the remote source segments: either a multi-segment scatter source
  // (attachment_regions) or the single-region source (addr/length/rkey).
  struct Seg {
    uint64_t addr;
    uint32_t length;
    uint32_t rkey;
  };
  const auto& meta = cntl->request_meta();
  std::vector<Seg> segments;
  if (meta.attachment_regions_size() > 0) {
    segments.reserve(meta.attachment_regions_size());
    for (const auto& r : meta.attachment_regions()) {
      segments.push_back({r.addr(), r.length(), r.rkey()});
    }
  } else {
    segments.push_back({meta.addr(), meta.length(), meta.rkey()});
  }

  size_t total = 0;
  for (const auto& seg : segments) {
    if (seg.addr == 0 || seg.rkey == 0) {
      slab_pool.Free(buffer);
      OnError(cntl, ErrorCode::ProtocolError,
              "request rdma memory context is missing");
      return;
    }
    total += seg.length;
  }
  if (total != attachment_size) {
    slab_pool.Free(buffer);
    OnError(cntl, ErrorCode::ProtocolError,
            "request attachment size mismatches advertised rdma regions");
    return;
  }

  // One RDMA read per segment, each into its slice of the contiguous slab.
  cntl->request_attachment_read().reset(static_cast<int>(segments.size()));
  std::vector<WorkRequstContext> ctxs(segments.size());
  std::vector<SendWorkRequest> wrs;
  wrs.reserve(segments.size());
  uint64_t offset = 0;
  for (size_t i = 0; i < segments.size(); ++i) {
    const auto& seg = segments[i];
    SendWorkRequest wr;
    wr.opcode = OpCode::kRDMARead;
    wr.signaled = true;
    wr.addr =
        reinterpret_cast<uint64_t>(static_cast<char*>(buffer->data) + offset);
    wr.length = seg.length;
    wr.lkey = buffer->meta;
    wr.raddr = seg.addr;
    wr.rkey = seg.rkey;
    wr.ctx = &ctxs[i];
    wr.ctx->on_completion = [this, cntl](const WorkCompletion& wc) {
      if (!wc.status.ok()) {
        OnError(cntl, ErrorCode::QueuePairError, wc.status.ToString());
      }
      cntl->request_attachment_read().signal();
    };
    wrs.push_back(wr);
    offset += seg.length;
  }

  auto status = conn_->PostSendWorkRequests(wrs);
  if (!status.ok()) {
    slab_pool.Free(buffer);
    OnError(cntl, ErrorCode::QueuePairError, status.ToString());
    return;
  }

  cntl->request_attachment_read().wait();
  if (cntl->Failed()) {
    slab_pool.Free(buffer);
    return;
  }

  IOBuffer attachment;
  attachment.AppendUserDataWithMeta(
      buffer->data, attachment_size,
      [buffer](void*) { dingofs::cache::GetGlobalRecvSlabPool().Free(buffer); },
      buffer->meta);
  cntl->request_attachment() = std::move(attachment);
}

class BlockingClosure : public ::google::protobuf::Closure {
 public:
  void Wait() { event_.wait(); }
  void Run() override { event_.signal(); }

 private:
  bthread::CountdownEvent event_{1};
};

void ServerSession::ProcessRequest(Controller* cntl) {
  BlockingClosure done;
  cntl->service()->CallMethod(cntl->method(), cntl, cntl->request(),
                              cntl->response(), &done);
  done.Wait();
}

void ServerSession::SendResponse(Controller* cntl) {
  std::vector<SendWorkRequest> entries;
  auto& attachment = cntl->response_attachment();
  WorkRequstContext rdma_write_ctx;
  WorkRequstContext send_ctx;

  // part1: attchment
  if (!cntl->Failed() && attachment.Length() > 0) {
    if (attachment.Length() > cntl->request_meta().length()) {
      OnError(cntl, ErrorCode::ProtocolError,
              "response attachment exceeds advertised rdma length");
    } else if (cntl->request_meta().addr() == 0 ||
               cntl->request_meta().rkey() == 0) {
      OnError(cntl, ErrorCode::ProtocolError,
              "response rdma memory context is missing");
    } else {
      SendWorkRequest wr;
      wr.opcode = OpCode::kRDMAWrite;
      wr.addr = reinterpret_cast<uint64_t>(attachment.Fetch1());
      wr.length = static_cast<uint32_t>(attachment.Length());
      wr.lkey = attachment.GetFirstDataMeta();
      wr.raddr = cntl->request_meta().addr();
      wr.rkey = cntl->request_meta().rkey();
      wr.signaled = false;
      wr.ctx = &rdma_write_ctx;

      entries.emplace_back(wr);
    }
  }

  // part2: response
  auto* send_buffer_pool = conn_->GetSendBufferPool();
  auto* send_buffer = send_buffer_pool->Alloc();
  if (send_buffer == nullptr) {
    LOG(ERROR) << "Fail to send response because send buffer is exhausted";
    OnError(cntl, ErrorCode::NoMem, "alloc send buffer failed");
    return;
  }

  BRPC_SCOPE_EXIT { send_buffer_pool->Free(send_buffer); };

  pb::infiniband::ResponseMeta response_meta = cntl->response_meta();
  response_meta.set_error_code(cntl->ErrorCode());
  *response_meta.mutable_error_message() = cntl->ErrorText();
  if (!cntl->Failed() && attachment.Length() > 0) {
    response_meta.set_attachment_size(
        static_cast<uint32_t>(attachment.Length()));
  } else {
    response_meta.set_attachment_size(0);
  }

  auto status = Protocol::SerializeResponse(
      cntl->correlation_id(), response_meta, cntl->response(), send_buffer);
  if (!status.ok()) {
    OnError(cntl, ErrorCode::ProtocolError, status.ToString());
    return;
  }

  SendWorkRequest wr;
  wr.addr = reinterpret_cast<uint64_t>(send_buffer->data);
  wr.length = send_buffer->length;
  wr.lkey = send_buffer->lkey;
  wr.opcode = OpCode::kSend;
  wr.signaled = true;
  wr.ctx = &send_ctx;
  wr.ctx->on_completion = [this, cntl](const WorkCompletion& wc) {
    if (!wc.status.ok()) {
      OnError(cntl, ErrorCode::QueuePairError, wc.status.ToString());
    }
    cntl->response_sent().signal();
  };
  entries.emplace_back(wr);

  status = conn_->PostSendWorkRequests(entries);
  if (!status.ok()) {
    OnError(cntl, ErrorCode::QueuePairError, status.ToString());
    return;
  }

  cntl->response_sent().wait();  // We will release buffer on response sent
}

void ServerSession::OnError(Controller* cntl,
                            pb::infiniband::ErrorCode error_code,
                            const std::string& reason) {
  cntl->SetErrorCode(error_code);
  cntl->SetFailed(reason);
  LOG(ERROR) << "Fail to process request: " << reason;
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
