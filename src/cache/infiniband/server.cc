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

#include "cache/infiniband/server.h"

#include <brpc/controller.h>
#include <glog/logging.h>

#include <memory>
#include <utility>

#include "cache/infiniband/connection.h"
#include "cache/infiniband/event.h"
#include "cache/infiniband/infiniband.h"
#include "cache/infiniband/server_session.h"
#include "cache/infiniband/service.h"
#include "common/status.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

Status Listener::Listen(const EndPoint& ep) {
  Infiniband::Context context;
  auto status = Infiniband::Init(ep.device_name, ep.port_num, &context);
  if (!status.ok()) {
    return status;
  }

  device_ = context.device;
  port_ = context.port;
  protect_domain_ = context.protect_domain;

  LOG(INFO) << "Infiniband listener is listening on " << ep.device_name << ":"
            << static_cast<int>(ep.port_num);
  return Status::OK();
}

ConnectionUPtr Listener::Accept(const ConnManagmentMeta& remote_cm_meta) {
  LOG(INFO) << "Accepting infiniband connection: peer=" << remote_cm_meta;

  auto completion_queue = CompletionQueue::Create(device_);
  if (nullptr == completion_queue) {
    LOG(ERROR) << "Fail to create completion queue";
    return nullptr;
  }

  auto queue_pair = QueuePair::Create(device_, port_, protect_domain_,
                                      completion_queue.get());
  if (nullptr == queue_pair) {
    LOG(ERROR) << "Fail to create queue pair";
    return nullptr;
  }

  auto status = queue_pair->ModifyQpToInit();
  if (!status.ok()) {
    return nullptr;
  }

  status = queue_pair->ModifyQpToRtr(remote_cm_meta);
  if (!status.ok()) {
    return nullptr;
  }

  status = queue_pair->ModifyQpToRts();
  if (!status.ok()) {
    return nullptr;
  }

  return std::make_unique<Connection>(std::move(queue_pair),
                                      std::move(completion_queue));
}

InfinibandServiceImpl::InfinibandServiceImpl(Listener* listener,
                                             ServiceHub* service_hub)
    : listener_(listener), service_hub_(service_hub) {
  CHECK_NOTNULL(listener_);
  CHECK_NOTNULL(service_hub);
}

InfinibandServiceImpl::~InfinibandServiceImpl() { CloseAllSessions(); }

void InfinibandServiceImpl::AddSession(ServerSessionSPtr session) {
  std::lock_guard<bthread::Mutex> guard(mutex_);
  sessions_.emplace_back(std::move(session));
}

void InfinibandServiceImpl::CloseAllSessions() {
  std::lock_guard<bthread::Mutex> guard(mutex_);
  for (auto& session : sessions_) {
    session->Shutdown();
  }
}

void InfinibandServiceImpl::Sync(google::protobuf::RpcController* controller,
                                 const pb::infiniband::SyncRequest* request,
                                 pb::infiniband::SyncResponse* response,
                                 google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto* cntl = static_cast<brpc::Controller*>(controller);

  // cm meta
  ConnManagmentMeta remote_cm_meta;
  auto status = ParseFromPb(request->cm_meta(), &remote_cm_meta);
  if (!status.ok()) {
    cntl->SetFailed(status.ToString());
    LOG(ERROR) << "Fail to parse conn managment meta proto buffer";
    return;
  }

  // connection
  auto conn = listener_->Accept(remote_cm_meta);
  if (conn == nullptr) {
    cntl->SetFailed("accept connection failed");
    LOG(ERROR) << "Fail to accept connection";
    return;
  }

  // session
  int fd = conn->GetFd();
  auto* qp = conn->GetQueuePair();
  ConnManagmentMeta local_cm_meta = qp->GetConnManagmentMeta();
  auto session = std::make_shared<ServerSession>(std::move(conn), service_hub_);
  status = session->Start();
  if (!status.ok()) {
    cntl->SetFailed("establish session failed: " + status.ToString());
    LOG(ERROR) << "Fail to establish session: " << status.ToString();
    return;
  }

  // add to event
  status =
      GetGlobalEventDispatcher(fd).AddEvent(fd, EventType::kReadEvent, session);
  if (!status.ok()) {
    session->Shutdown();
    cntl->SetFailed("register event failed: " + status.ToString());
    LOG(ERROR) << "Fail to register event: " << status.ToString();
    return;
  }

  SerializeToPb(local_cm_meta, response->mutable_cm_meta());
  AddSession(std::move(session));

  LOG(INFO) << "Accepted RDMA connection: peer=" << remote_cm_meta
            << " local=" << local_cm_meta << " fd=" << fd;
}

Server::Server()
    : listener_(std::make_unique<Listener>()),
      service_hub_(std::make_unique<ServiceHub>()) {}

Status Server::Start(const EndPoint& ep, ServerOptions* options) {
  auto* brpc_server = options->brpc_server;
  CHECK_NOTNULL(brpc_server);  // TODO: support null brpc_server

  auto status = listener_->Listen(ep);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to start listener: " << status.ToString();
    return status;
  }

  service_ = std::make_unique<InfinibandServiceImpl>(listener_.get(),
                                                     service_hub_.get());

  int rc =
      brpc_server->AddService(service_.get(), brpc::SERVER_DOESNT_OWN_SERVICE);
  if (rc != 0) {
    LOG(ERROR) << "Fail to add InfinibandService to brpc server";
    return Status::Internal("add service failed");
  }

  LOG(INFO) << "Infiniband server is up.";
  return Status::OK();
}

void Server::Shutdown() { LOG(INFO) << "RDMA server is shutdown"; }

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
