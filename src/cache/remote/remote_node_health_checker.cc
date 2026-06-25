/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2025-01-21
 * Author: Jingli Chen (Wine93)
 */

#include "cache/remote/remote_node_health_checker.h"

#include "cache/iutil/state_machine_impl.h"
#include "cache/remote/remote_node.h"
#include "dingofs/blockcache.pb.h"
#include "utils/executor/bthread/bthread_executor.h"

namespace dingofs {
namespace cache {

DEFINE_uint32(cache_ping_rpc_timeout_ms, 1000,
              "timeout for pinging a remote cache node in milliseconds");
DEFINE_validator(cache_ping_rpc_timeout_ms, brpc::PassValidate);

DEFINE_uint32(cache_node_state_check_duration_ms, 3000,
              "interval for checking remote cache-node health in milliseconds");
DEFINE_validator(cache_node_state_check_duration_ms, brpc::PassValidate);

DEFINE_uint32(cache_node_state_tick_duration_s, 30,
              "state-machine tick duration for remote cache-node health "
              "transitions in seconds");
DEFINE_validator(cache_node_state_tick_duration_s, brpc::PassValidate);

DEFINE_uint32(
    cache_node_state_normal2unstable_error_num, 10,
    "number of errors needed to move a remote cache node from normal to "
    "unstable");
DEFINE_validator(cache_node_state_normal2unstable_error_num,
                 brpc::PassValidate);

DEFINE_uint32(cache_node_state_unstable2normal_succ_num, 3,
              "number of successful pings needed to move from unstable to "
              "normal");
DEFINE_validator(cache_node_state_unstable2normal_succ_num, brpc::PassValidate);

DEFINE_uint32(
    cache_node_state_unstable2down_s, 604800,  // 7 days
    "time in unstable state before moving a remote cache node to down "
    "in seconds");
DEFINE_validator(cache_node_state_unstable2down_s, brpc::PassValidate);

namespace {

struct Configure : public iutil::IConfiguration {
  int tick_duration_s() override {
    return FLAGS_cache_node_state_tick_duration_s;
  }

  int normal2unstable_error_num() override {
    return FLAGS_cache_node_state_normal2unstable_error_num;
  }

  int unstable2normal_succ_num() override {
    return FLAGS_cache_node_state_unstable2normal_succ_num;
  }

  int unstable2down_s() override {
    return FLAGS_cache_node_state_unstable2down_s;
  }
};

}  // namespace

RemoteNodeHealthChecker::RemoteNodeHealthChecker(const std::string& ip,
                                                 uint32_t port)
    : running_(false),
      ip_(ip),
      port_(port),
      executor_(std::make_unique<BthreadExecutor>()),
      state_machine_(std::make_unique<iutil::StateMachineImpl>(
          std::make_unique<Configure>())),
      conn_(RemoteNodeConnection::New()) {}

void RemoteNodeHealthChecker::Start() {
  if (running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "RemoteNodeHealthChecker already started";
    return;
  }

  LOG(INFO) << "RemoteNodeHealthChecker is starting...";

  CHECK(state_machine_->Start());
  CHECK(executor_->Start());
  executor_->Schedule([this] { PeriodicCheckNode(); },
                      FLAGS_cache_node_state_check_duration_ms);
  executor_->Schedule([this] { PeriodicCommitStageIOResult(); }, 1000);

  running_.store(true, std::memory_order_relaxed);
  LOG(INFO) << "RemoteNodeHealthChecker started, start checking " << ip_ << ":"
            << port_;
}

void RemoteNodeHealthChecker::Shutdown() {
  if (!running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "RemoteNodeHealthChecker already shutdown";
    return;
  }

  LOG(INFO) << "RemoteNodeHealthChecker is shutting down...";

  CHECK(executor_->Stop());
  CHECK(state_machine_->Shutdown());

  running_.store(false, std::memory_order_relaxed);
  LOG(INFO) << "RemoteNodeHealthChecker is down, stop checking " << ip_ << ":"
            << port_;
}

Status RemoteNodeHealthChecker::SendPingRequest() {
  auto timeout_ms = FLAGS_cache_ping_rpc_timeout_ms;
  if (!conn_->IsConnected()) {
    auto status = conn_->Connect(ip_, port_, timeout_ms);
    if (!status.ok()) {
      LOG(ERROR) << "Fail to connect to cache node, endpoint=" << ip_ << ":"
                 << port_;
      return status;
    }
  }

  pb::cache::PingRequest request;
  pb::cache::PingResponse response;
  RemoteNodeConnection::Result result;
  conn_->Send("Ping", request, &response, nullptr, nullptr, timeout_ms,
              &result);
  if (result.failed) {
    LOG(ERROR) << "Fail to send ping request to node: " << result.error_text;
    conn_->Close();
    return Status::NetError(result.error_code, result.error_text);
  }
  return Status::OK();
}

void RemoteNodeHealthChecker::CheckNode() {
  auto status = SendPingRequest();
  if (status.ok()) {
    state_machine_->Success();
  } else {
    state_machine_->Error();
    LOG(ERROR) << "Fail to check node health";
  }
}

void RemoteNodeHealthChecker::PeriodicCheckNode() {
  CheckNode();
  executor_->Schedule([this] { PeriodicCheckNode(); },
                      FLAGS_cache_node_state_check_duration_ms);
}

void RemoteNodeHealthChecker::CommitStageIOResult() {
  auto nerror = num_stage_error_.exchange(0, std::memory_order_relaxed);
  auto nsuccess = num_stage_success_.exchange(0, std::memory_order_relaxed);
  if (nerror > nsuccess) {
    state_machine_->Error(nerror - nsuccess);
  } else if (nsuccess > nerror) {
    state_machine_->Success(nsuccess - nerror);
  }

  if (state_machine_->GetState() == iutil::State::kStateNormal) {
    is_healthy_.store(true);
  } else {
    is_healthy_.store(false);
  }
}

void RemoteNodeHealthChecker::PeriodicCommitStageIOResult() {
  CommitStageIOResult();
  executor_->Schedule([this] { PeriodicCommitStageIOResult(); }, 1000);
}

}  // namespace cache
}  // namespace dingofs
