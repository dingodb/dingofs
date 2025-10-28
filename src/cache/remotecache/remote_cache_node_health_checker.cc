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

/*
 * Project: DingoFS
 * Created Date: 2025-02-10
 * Author: Jingli Chen (Wine93)
 */

#include "cache/remotecache/remote_cache_node_health_checker.h"

#include <absl/strings/str_format.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/reloadable_flags.h>
#include <glog/logging.h>

#include <memory>

#include "cache/common/macro.h"
#include "cache/common/mds_client.h"
#include "cache/common/state_machine.h"
#include "cache/metric/cache_status.h"
#include "common/options/cache/option.h"
#include "dingofs/blockcache.pb.h"
#include "utils/executor/bthread/bthread_executor.h"

namespace dingofs {
namespace cache {

DEFINE_uint32(cache_node_state_check_duration_ms, 3000,
              "Duration in milliseconds to check the cache group node state");
DEFINE_validator(cache_node_state_check_duration_ms, brpc::PassValidate);

DEFINE_uint32(ping_rpc_timeout_ms, 1000,
              "RPC timeout for pinging remote cache node in milliseconds");
DEFINE_validator(ping_rpc_timeout_ms, brpc::PassValidate);

RemoteCacheNodeHealthChecker::RemoteCacheNodeHealthChecker(
    const CacheGroupMember& member, StateMachineSPtr state_machine)
    : running_(false),
      member_(member),
      state_machine_(state_machine),
      executor_(std::make_unique<BthreadExecutor>()),
      channel_(std::make_unique<brpc::Channel>()) {}

void RemoteCacheNodeHealthChecker::Start() {
  CHECK_NOTNULL(state_machine_);
  CHECK_NOTNULL(executor_);
  CHECK_NOTNULL(channel_);

  if (running_) {
    return;
  }

  LOG(INFO) << "Remote cache node health checker is starting...";

  InitChannel();
  CHECK(state_machine_->Start([this](State state) { SetStatusPage(state); }));
  CHECK(executor_->Start());
  executor_->Schedule([this] { RunCheck(); },
                      FLAGS_cache_node_state_check_duration_ms);

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
                      FLAGS_cache_node_state_check_duration_ms);
}

void RemoteCacheNodeHealthChecker::PingNode() {
  auto status = SendPingRequest();
  if (!status.ok()) {
    state_machine_->Error();
  } else {
    state_machine_->Success();
  }
  SetStatusPage(state_machine_->GetState());
}

void RemoteCacheNodeHealthChecker::InitChannel() {
  butil::EndPoint endpoint;
  butil::str2endpoint(member_.ip.c_str(), member_.port, &endpoint);

  brpc::ChannelOptions options;
  options.connect_timeout_ms = FLAGS_rpc_connect_timeout_ms;
  options.connection_group = "urgent";

  auto* channel = channel_.get();
  int rc = channel->Init(endpoint, &options);
  if (rc != 0) {
    LOG(ERROR) << "Initialize channel failed: endpoint = " << member_.ip << ":"
               << member_.port << ", rc = " << rc;
  } else {
    LOG(INFO) << "Initialize channel success: endpoint = " << member_.ip << ":"
              << member_.port;
  }
}

void RemoteCacheNodeHealthChecker::ResetChannel() { InitChannel(); }

Status RemoteCacheNodeHealthChecker::SendPingRequest() {
  brpc::Controller cntl;
  cntl.ignore_eovercrowded();
  cntl.set_timeout_ms(FLAGS_ping_rpc_timeout_ms);

  pb::cache::PingRequest request;
  pb::cache::PingResponse reponse;
  pb::cache::BlockCacheService_Stub stub(channel_.get());
  stub.Ping(&cntl, &request, &reponse, nullptr);
  if (cntl.Failed()) {
    LOG(ERROR) << "Send ping request to cache node failed: member = "
               << member_.ToString() << ", error_text = " << cntl.ErrorText();
    ResetChannel();
    return Status::NetError(cntl.ErrorCode(), cntl.ErrorText());
  }
  return Status::OK();
}

void RemoteCacheNodeHealthChecker::SetStatusPage(State new_state) {
  CacheStatus::Update([&](CacheStatus::Root& root) {
    root.remote_cache.nodes[member_.id].health = StateToString(new_state);
  });
}

}  // namespace cache
}  // namespace dingofs
