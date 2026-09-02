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
#include <bthread/bthread.h>
#include <butil/time.h>
#include <bvar/bvar.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <cerrno>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

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

DEFINE_uint32(rdma_server_keepalive_interval_s, 10,
              "interval in seconds between server keepalives sent on every "
              "rdma session to detect vanished peers; 0 disables keepalive");

static bvar::Adder<int64_t> g_sessions("dingofs_rdma_server_sessions");
static bvar::Adder<int64_t> g_sessions_accepted(
    "dingofs_rdma_server_sessions_accepted");
static bvar::Adder<int64_t> g_sessions_reaped(
    "dingofs_rdma_server_sessions_reaped");

static constexpr int64_t kSweepIntervalMs = 1000;

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

ConnectionUPtr Listener::Accept(const ConnManagementMeta& remote_cm_meta) {
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

  return Connection::Create(std::move(queue_pair), std::move(completion_queue));
}

InfinibandServiceImpl::InfinibandServiceImpl(Listener* listener,
                                             ServiceHub* service_hub)
    : listener_(listener), service_hub_(service_hub) {
  CHECK_NOTNULL(listener_);
  CHECK_NOTNULL(service_hub);
}

InfinibandServiceImpl::~InfinibandServiceImpl() { Shutdown(); }

void InfinibandServiceImpl::Start() {
  if (running_.exchange(true, std::memory_order_acq_rel)) {
    return;
  }
  stopped_.reset(1);
  CHECK_EQ(0, bthread_start_background(&sweeper_, nullptr, RunSweeper, this))
      << "Fail to start rdma session sweeper";
}

void InfinibandServiceImpl::Shutdown() {
  if (!running_.exchange(false, std::memory_order_acq_rel)) {
    return;
  }

  stopped_.signal();
  bthread_join(sweeper_, nullptr);

  std::unordered_map<ServerSession*, ServerSessionSPtr> sessions;
  {
    std::lock_guard<bthread::Mutex> guard(mutex_);
    sessions.swap(sessions_);
  }
  for (auto& [_, session] : sessions) {
    session->Shutdown();
  }
  g_sessions << -static_cast<int64_t>(sessions.size());

  LOG(INFO) << "Infiniband service is shutdown, closed " << sessions.size()
            << " sessions";
}

void* InfinibandServiceImpl::RunSweeper(void* meta) {
  static_cast<InfinibandServiceImpl*>(meta)->Sweep();
  return nullptr;
}

void InfinibandServiceImpl::Sweep() {
  int64_t last_keepalive_ms = butil::monotonic_time_ms();
  while (running_.load(std::memory_order_acquire)) {
    if (stopped_.timed_wait(butil::milliseconds_from_now(kSweepIntervalMs)) ==
        0) {
      break;
    }

    ReapBrokenSessions();

    int64_t interval_ms =
        static_cast<int64_t>(FLAGS_rdma_server_keepalive_interval_s) * 1000;
    int64_t now_ms = butil::monotonic_time_ms();
    if (interval_ms > 0 && now_ms - last_keepalive_ms >= interval_ms) {
      SendKeepalives();
      last_keepalive_ms = now_ms;
    }
  }
}

bool InfinibandServiceImpl::AddSession(ServerSessionSPtr session) {
  std::lock_guard<bthread::Mutex> guard(mutex_);
  if (!running_.load(std::memory_order_acquire)) {
    return false;
  }
  sessions_.emplace(session.get(), std::move(session));
  g_sessions << 1;
  g_sessions_accepted << 1;
  return true;
}

void InfinibandServiceImpl::ReapBrokenSessions() {
  std::vector<ServerSessionSPtr> broken;
  {
    std::lock_guard<bthread::Mutex> guard(mutex_);
    for (auto iter = sessions_.begin(); iter != sessions_.end();) {
      if (iter->second->IsBroken()) {
        broken.emplace_back(std::move(iter->second));
        iter = sessions_.erase(iter);
      } else {
        ++iter;
      }
    }
  }

  if (broken.empty()) {
    return;
  }

  for (auto& session : broken) {
    session->Shutdown();
  }
  g_sessions << -static_cast<int64_t>(broken.size());
  g_sessions_reaped << broken.size();

  LOG(INFO) << "Reaped " << broken.size() << " broken rdma sessions, "
            << g_sessions.get_value() << " sessions left";
}

void InfinibandServiceImpl::SendKeepalives() {
  std::vector<ServerSessionSPtr> sessions;
  {
    std::lock_guard<bthread::Mutex> guard(mutex_);
    sessions.reserve(sessions_.size());
    for (const auto& [_, session] : sessions_) {
      sessions.emplace_back(session);
    }
  }

  for (auto& session : sessions) {
    session->SendKeepalive();
  }
}

void InfinibandServiceImpl::Sync(google::protobuf::RpcController* controller,
                                 const pb::infiniband::SyncRequest* request,
                                 pb::infiniband::SyncResponse* response,
                                 google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto* cntl = static_cast<brpc::Controller*>(controller);

  if (!running_.load(std::memory_order_acquire)) {
    cntl->SetFailed("infiniband service is not running");
    return;
  }

  // cm meta
  ConnManagementMeta remote_cm_meta;
  auto status = ParseFromPb(request->cm_meta(), &remote_cm_meta);
  if (!status.ok()) {
    cntl->SetFailed(status.ToString());
    LOG(ERROR) << "Fail to parse conn management meta proto buffer";
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
  ConnManagementMeta local_cm_meta = qp->GetConnManagementMeta();
  auto session = std::make_shared<ServerSession>(std::move(conn), service_hub_);
  status = session->Start();
  if (!status.ok()) {
    session->Shutdown();
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

  if (!AddSession(session)) {
    session->Shutdown();
    cntl->SetFailed("infiniband service is shutting down");
    LOG(WARNING) << "Reject connection because service is shutting down";
    return;
  }

  SerializeToPb(local_cm_meta, response->mutable_cm_meta());

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

  service_->Start();

  LOG(INFO) << "Infiniband server is up.";
  return Status::OK();
}

void Server::Shutdown() {
  if (service_ != nullptr) {
    service_->Shutdown();
  }
  LOG(INFO) << "RDMA server is shutdown";
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
