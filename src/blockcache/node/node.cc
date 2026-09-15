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

#include "blockcache/node/node.h"

#include <arpa/inet.h>
#include <brpc/controller.h>
#include <butil/memory/scope_guard.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <netinet/in.h>

#include <chrono>
#include <csignal>
#include <memory>
#include <thread>
#include <utility>

#include "blockcache/core/runtime/smp.h"
#include "blockcache/utils/string.h"
#include "common/options/cache.h"

namespace brpc {
DECLARE_bool(graceful_quit_on_sigterm);
}  // namespace brpc

namespace dingofs {
namespace blockcache {

DEFINE_string(id, "", "cache node id");
DEFINE_validator(id, [](const char* /*name*/, const std::string& value) {
  return !value.empty();
});

DEFINE_string(listen_ip, "", "ip to listen on");
DEFINE_validator(listen_ip, [](const char* /*name*/, const std::string& value) {
  const std::string ip = TrimWhitespace(value);
  in_addr addr{};
  return !ip.empty() && ::inet_pton(AF_INET, ip.c_str(), &addr) == 1 &&
         addr.s_addr != htonl(INADDR_ANY);
});

DEFINE_uint32(listen_port, 9300, "port to listen on");

DEFINE_bool(bind_all, true, "bind 0.0.0.0 instead of listen_ip");

DEFINE_bool(daemonize, false, "run in background");

static std::string BindIp() {
  return FLAGS_bind_all ? "0.0.0.0" : FLAGS_listen_ip;
}

CacheNode::CacheNode() : CacheNode(std::make_unique<MDSClientImpl>()) {}

CacheNode::CacheNode(MDSClientUPtr mds_client)
    : mds_client_(std::move(mds_client)),
      block_cache_(std::make_unique<ShardedLocalCache>(mds_client_.get())),
      cache_service_(block_cache_.get()),
      membership_(std::make_unique<GroupMembership>(mds_client_.get())),
      heartbeat_(std::make_unique<Heartbeat>(mds_client_.get())) {
  FLAGS_cache_dir_uuid = FLAGS_id;
  FLAGS_listen_ip = TrimWhitespace(FLAGS_listen_ip);

  // brpc server
  {
    BrpcServer::Option option;
    option.listen_ip = BindIp();
    option.listen_port = static_cast<uint16_t>(FLAGS_listen_port);
    brpc_server_ = std::make_unique<BrpcServer>(option);
  }

  // infiniband server
  {
    if (!FLAGS_use_rdma) {
      return;
    }
    CHECK(FLAGS_rdma_idle_timeout_s >= 3 * FLAGS_rdma_heartbeat_interval_s)
        << "rdma_idle_timeout_s must be at least 3x rdma_heartbeat_interval_s";

    infiniband::ServerOption option;
    option.device_name = FLAGS_cache_rdma_device;
    option.brpc_server = brpc_server_.get();
    infiniband_server_ =
        std::make_unique<infiniband::Server>(std::move(option));
  }
}

CacheNode::~CacheNode() { Shutdown(); }

Status CacheNode::Start() {
  CHECK(!running_) << "CacheNode started twice";
  CHECK(ShardCount() > 0) << "the runtime must be up before CacheNode::Start";

  LOG(INFO) << "CacheNode is starting...";

  running_ = true;
  Status status;
  BRPC_SCOPE_EXIT {
    if (!status.ok()) {
      Shutdown();
    }
  };

  status = mds_client_->Start();
  if (!status.ok()) {
    return status;
  }

  status = block_cache_->Start();
  if (!status.ok()) {
    return status;
  }

  status = StartServers();
  if (!status.ok()) {
    return status;
  }

  status = membership_->Start();
  if (!status.ok()) {
    return status;
  }

  heartbeat_->Start();

  LOG(INFO) << "Successfully start CacheNode{id=" << FLAGS_id
            << " listen_address=" << FLAGS_listen_ip << ":" << FLAGS_listen_port
            << " shards=" << ShardCount()
            << " rdma=" << (FLAGS_use_rdma ? "on" : "off") << "}";
  return Status::OK();
}

void CacheNode::Shutdown() {
  if (!running_) {
    return;
  }

  LOG(INFO) << "CacheNode is shutting down...";

  heartbeat_->Shutdown();
  membership_->Shutdown();
  ShutdownServers();
  block_cache_->Shutdown();
  mds_client_->Shutdown();

  running_ = false;
  LOG(INFO) << "Successfully shutdown CacheNode";
}

void CacheNode::RunUntilAskedToQuit() {
  brpc::FLAGS_graceful_quit_on_sigterm = true;
  while (!brpc::IsAskedToQuit()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  (void)std::signal(SIGINT, SIG_DFL);
  (void)std::signal(SIGTERM, SIG_DFL);

  LOG(INFO) << "Asked to quit, stopping the node";
}

Status CacheNode::StartServers() {
  Status status = StartInfinibandServer();
  if (!status.ok()) {
    return status;
  }
  return StartBrpcServer();
}

void CacheNode::ShutdownServers() {
  brpc_server_->Shutdown();
  if (infiniband_server_ != nullptr) {
    infiniband_server_->Shutdown();
  }
}

Status CacheNode::StartInfinibandServer() {
  if (infiniband_server_ == nullptr) {
    return Status::OK();
  }

  infiniband_server_->AddService(&cache_service_);
  Status status = infiniband_server_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to start infiniband server at "
               << FLAGS_cache_rdma_device << ":" << FLAGS_cache_rdma_port_num
               << ": " << status.ToString();
  }
  return status;
}

Status CacheNode::StartBrpcServer() {
  brpc_server_->AddService(
      std::make_unique<RawCacheService>(brpc_server_.get(), &cache_service_));
  Status status = brpc_server_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to start brpc server at " << BindIp() << ":"
               << FLAGS_listen_port << ": " << status.ToString();
  }
  return status;
}

}  // namespace blockcache
}  // namespace dingofs
