// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "cache/remotecache/remote_node_health_checker.h"

#include <absl/strings/str_format.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/reloadable_flags.h>

#include "cache/common/macro.h"
#include "cache/common/proto.h"
#include "utils/executor/bthread/bthread_executor.h"

namespace dingofs {
namespace cache {

DEFINE_uint32(check_cache_node_state_duration_ms, 3000,
              "Duration in milliseconds to check the cache group node state");
DEFINE_validator(check_cache_node_state_duration_ms, brpc::PassValidate);

DEFINE_uint32(ping_rpc_timeout_ms, 3000,
              "RPC timeout for pinging remote cache node in milliseconds");
DEFINE_validator(ping_rpc_timeout_ms, brpc::PassValidate);

RemoteCacheNodeHealthChecker::RemoteCacheNodeHealthChecker(
    const PBCacheGroupMember& member, StateMachineSPtr state_machine)
    : running_(false),
      member_info_(member),
      state_machine_(state_machine),
      executor_(std::make_unique<BthreadExecutor>()) {}

void RemoteCacheNodeHealthChecker::Start() {
  if (running_) {
    return;
  }

  LOG(INFO) << "Remote cache node health checker is starting...";

  CHECK(state_machine_->Start());
  CHECK(executor_->Start());
  executor_->Schedule([this] { RunCheck(); },
                      FLAGS_check_cache_node_state_duration_ms);

  running_ = true;

  LOG(INFO) << "Remote cache node health checker is up.";

  CHECK_RUNNING("Remote cache node health checker");
}

void RemoteCacheNodeHealthChecker::Shutdown() {
  if (!running_.exchange(false)) {
    return;
  }

  LOG(INFO) << "Remote cache node health checker is shutting down...";

  executor_->Stop();
  state_machine_->Shutdown();

  LOG(INFO) << "Remote cache node health checker is down.";

  CHECK_DOWN("Remote cache node health checker");
}

void RemoteCacheNodeHealthChecker::RunCheck() {
  PingNode();
  executor_->Schedule([this] { RunCheck(); },
                      FLAGS_check_cache_node_state_duration_ms);
}

void RemoteCacheNodeHealthChecker::PingNode() {
  auto status = SendPingrequest();
  if (!status.ok()) {
    state_machine_->Error();
  } else {
    state_machine_->Success();
  }
}

Status RemoteCacheNodeHealthChecker::SendPingrequest() {
  brpc::Channel channel;
  PBPingRequest request;
  PBPingResponse reponse;
  brpc::Controller cntl;
  butil::EndPoint endpoint;
  butil::str2endpoint(member_info_.ip().c_str(), member_info_.port(),
                      &endpoint);

  int rc = channel.Init(endpoint, nullptr);
  if (rc != 0) {
    LOG(ERROR) << "Initialize channel failed: endpoint = " << member_info_.ip()
               << ":" << member_info_.port() << ", rc = " << rc;
    return Status::Internal(absl::StrFormat(
        "init channel (%s:%d) failed", member_info_.ip(), member_info_.port()));
  }

  cntl.ignore_eovercrowded();
  cntl.set_timeout_ms(FLAGS_ping_rpc_timeout_ms);

  PBBlockCacheService_Stub stub(&channel);
  stub.Ping(&cntl, &request, &reponse, nullptr);
  if (cntl.Failed()) {
    LOG(ERROR) << "Send ping request to cache node failed: endpoint = "
               << member_info_.ip() << ":" << member_info_.port()
               << ", error_text = " << cntl.ErrorText();
    return Status::NetError(cntl.ErrorCode(), cntl.ErrorText());
  }
  return Status::OK();
}

}  // namespace cache
}  // namespace dingofs
