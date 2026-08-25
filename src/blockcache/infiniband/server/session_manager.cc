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

#include "blockcache/infiniband/server/session_manager.h"

#include <glog/logging.h>

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <utility>

#include "blockcache/common/flag_decls.h"
#include "blockcache/common/status.h"
#include "blockcache/core/reactor/timer.h"
#include "blockcache/infiniband/base/completion_queue.h"
#include "blockcache/infiniband/base/device.h"
#include "blockcache/infiniband/common/protocol.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

static uint32_t WorkRequestsPerConnection() {
  return Protocol::MsgSendWr() + Protocol::MsgRecvWr() +
         (FLAGS_rdma_bulk_qps * FLAGS_rdma_bulk_send_wr);
}

static uint64_t IdleTimeoutNs() {
  return uint64_t{FLAGS_rdma_idle_timeout_s} * 1'000'000'000;
}

SessionManager::SessionManager(Device* device,
                               CompletionQueue* completion_queue)
    : device_(device), completion_queue_(completion_queue) {}

SessionManager::~SessionManager() {
  CHECK(sessions_.empty()) << "SessionManager destroyed before Shutdown()";
}

void SessionManager::Start() {
  LOG(INFO) << "SessionManager is starting...";

  cleanup_timer_ = std::make_unique<Timer>();
  cleanup_timer_->SetCallback([this] { (void)CleanupSessions(); });
  cleanup_timer_->ArmPeriodic(std::chrono::nanoseconds(IdleTimeoutNs() / 4));
  running_ = true;

  LOG(INFO) << "Successfully start SessionManager";
}

Future<> SessionManager::Shutdown() {
  LOG(INFO) << "SessionManager is shutting down...";

  running_ = false;

  cleanup_timer_->Cancel();

  for (const SessionUPtr& session : sessions_) {
    (void)co_await session->Shutdown();
  }

  co_await gate_.Close();
  for (const SessionUPtr& session : sessions_) {
    const bool succ = co_await session->Shutdown();
    CHECK(succ)
        << "a session outlived the gate with a receive slot still lent out";
  }
  sessions_.clear();

  LOG(INFO) << "Successfully shutdown SessionManager";
}

Future<Status> SessionManager::AddSession(SessionUPtr session) {
  Status status = completion_queue_->Reserve(WorkRequestsPerConnection(),
                                             device_->max_cqe());
  if (!status.ok()) {
    co_return status;
  }

  status = co_await session->Start();
  if (status.ok() && running_) {
    sessions_.push_back(std::move(session));
    co_return Status::OK();
  }

  completion_queue_->Unreserve(WorkRequestsPerConnection());
  (void)co_await session->Shutdown();
  if (!status.ok()) {
    co_return status;
  }
  co_return ToStatus(ECANCELED, "add a session: manager is stopping");
}

Future<> SessionManager::CleanupSessions() {
  Gate::Holder holder(gate_);
  if (!holder.ok()) {
    co_return;
  }

  for (const SessionUPtr& session : sessions_) {
    session->FailIfIdle(IdleTimeoutNs());
  }

  if (!running_ || cleanuping_) {
    co_return;
  }

  cleanuping_ = true;
  size_t count = 0;
  for (SessionUPtr& session : sessions_) {
    if (session->Alive()) {
      continue;
    }

    if (co_await session->Shutdown()) {
      session.reset();
      ++count;
    }
  }
  std::erase(sessions_, nullptr);
  cleanuping_ = false;

  completion_queue_->Unreserve(static_cast<uint32_t>(count) *
                               WorkRequestsPerConnection());
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
