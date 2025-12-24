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

#include "cache/remotecache/peer_health_checker.h"

#include "cache/iutil/state_machine_impl.h"
#include "cache/remotecache/peer.h"
#include "dingofs/blockcache.pb.h"
#include "utils/executor/bthread/bthread_executor.h"

namespace dingofs {
namespace cache {

DEFINE_uint32(cache_ping_rpc_timeout_ms, 1000,
              "rpc timeout for pinging remote cache node in milliseconds");
DEFINE_validator(cache_ping_rpc_timeout_ms, brpc::PassValidate);

DEFINE_uint32(cache_node_state_check_duration_ms, 3000,
              "duration in milliseconds to check the cache group node state");
DEFINE_validator(cache_node_state_check_duration_ms, brpc::PassValidate);

DEFINE_uint32(cache_node_state_tick_duration_s, 30,
              "duration in seconds for the cache node state tick");
DEFINE_validator(cache_node_state_tick_duration_s, brpc::PassValidate);

DEFINE_uint32(
    cache_node_state_normal2unstable_error_num, 10,
    "number of errors to trigger unstable cache state from normal state");
DEFINE_validator(cache_node_state_normal2unstable_error_num,
                 brpc::PassValidate);

DEFINE_uint32(cache_node_state_unstable2normal_succ_num, 3,
              "number of successes to trigger normal state from "
              "unstable state");
DEFINE_validator(cache_node_state_unstable2normal_succ_num, brpc::PassValidate);

DEFINE_uint32(cache_node_state_unstable2down_s, 604800,  // 7 days
              "duration in seconds to trigger down state from unstable state");
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

};  // namespace

PeerHealthChecker::PeerHealthChecker(const std::string& ip, uint32_t port)
    : running_(false),
      ip_(ip),
      port_(port),
      executor_(std::make_unique<BthreadExecutor>()),
      state_machine_(std::make_unique<iutil::StateMachineImpl>(Configure())),
      conn_(PeerConnection::New()) {}

void PeerHealthChecker::Start() {
  if (running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "PeerHealthChecker already started";
    return;
  }

  LOG(INFO) << "PeerHealthChecker is starting...";

  CHECK(state_machine_->Start());
  CHECK(executor_->Start());
  executor_->Schedule([this] { PeriodicCheckPeer(); },
                      FLAGS_cache_node_state_check_duration_ms);
  executor_->Schedule([this] { PeriodicCommitStageIOResult(); }, 1000);

  running_.store(true, std::memory_order_relaxed);
  LOG(INFO) << "PeerHealthChecker started, start checking " << ip_ << ":"
            << port_;
}

void PeerHealthChecker::Shutdown() {
  if (!running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "PeerHealthChecker already shutdown";
    return;
  }

  LOG(INFO) << "PeerHealthChecker is shutting down...";

  CHECK(executor_->Stop());
  CHECK(state_machine_->Shutdown());

  running_.store(false, std::memory_order_relaxed);
  LOG(INFO) << "PeerHealthChecker is down, stop checking " << ip_ << ":"
            << port_;
}

Status PeerHealthChecker::SendPingRequest() {
  auto timeout_ms = FLAGS_cache_ping_rpc_timeout_ms;
  Status status;
  auto* channel = conn_->GetChannel();
  if (nullptr == channel) {
    status = conn_->Connect(ip_, port_, timeout_ms);
    if (!status.ok()) {
      LOG(ERROR) << "Fail to connect to cache node, endpoint=" << ip_ << ":"
                 << port_;
      return status;
    }
  }

  channel = CHECK_NOTNULL(conn_->GetChannel());

  brpc::Controller cntl;
  cntl.ignore_eovercrowded();
  cntl.set_timeout_ms(timeout_ms);

  pb::cache::PingRequest request;
  pb::cache::PingResponse reponse;
  pb::cache::BlockCacheService_Stub stub(channel);
  stub.Ping(&cntl, &request, &reponse, nullptr);
  if (cntl.Failed()) {
    LOG(ERROR) << "Fail to send ping request to peer: " << cntl.ErrorText();
    conn_->Close();
    return Status::NetError(cntl.ErrorCode(), cntl.ErrorText());
  }
  return Status::OK();
}

void PeerHealthChecker::CheckPeer() {
  auto status = SendPingRequest();
  if (status.ok()) {
    state_machine_->Success();
  } else {
    state_machine_->Error();
    LOG(ERROR) << "Fail to check peer";
  }
}

void PeerHealthChecker::PeriodicCheckPeer() {
  CheckPeer();
  executor_->Schedule([this] { PeriodicCheckPeer(); },
                      FLAGS_cache_node_state_check_duration_ms);
}

void PeerHealthChecker::CommitStageIOResult() {
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

void PeerHealthChecker::PeriodicCommitStageIOResult() {
  CommitStageIOResult();
  executor_->Schedule([this] { PeriodicCommitStageIOResult(); }, 1000);
}

}  // namespace cache
}  // namespace dingofs
