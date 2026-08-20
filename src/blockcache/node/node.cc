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

#include <butil/memory/scope_guard.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <memory>
#include <utility>

#include "blockcache/common/flag_decls.h"
#include "blockcache/core/runtime/smp.h"
#include "blockcache/net/brpc/transport.h"

namespace dingofs {
namespace blockcache {

DEFINE_string(id, "", "cache node id");
DEFINE_validator(id, [](const char* /*name*/, const std::string& value) {
  return !value.empty();
});
DEFINE_string(listen_ip, "", "ip to listen on");
DEFINE_validator(listen_ip, [](const char* /*name*/, const std::string& value) {
  return !value.empty();
});
DEFINE_uint32(listen_port, 9300, "port to listen on");
DEFINE_string(bind_ip, "0.0.0.0", "ip to bind");
DEFINE_bool(rdma, false, "enable rdma transport");
DEFINE_string(rdma_device, "", "rdma device");
DEFINE_uint32(rdma_idle_timeout_s, 10,
              "seconds without hearing a peer before its connection is "
              "reaped; at least 3x the 2s ping interval");
DEFINE_bool(daemonize, false, "run in background");

CacheNode::CacheNode() : CacheNode(std::make_unique<MDSClientImpl>()) {}

CacheNode::CacheNode(MDSClientUPtr mds_client)
    : mds_client_(std::move(mds_client)),
      block_cache_(std::make_unique<ShardedLocalCache>(mds_client_.get())),
      membership_(std::make_unique<GroupMembership>(mds_client_.get())),
      heartbeat_(std::make_unique<Heartbeat>(mds_client_.get())) {
  FLAGS_cache_dir_uuid = FLAGS_id;
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

  status = block_cache_->Start();
  if (!status.ok()) {
    return status;
  }

  status = StartServer();
  if (!status.ok()) {
    return status;
  }

  status = membership_->Start();
  if (!status.ok()) {
    return status;
  }

  heartbeat_->Start();

  LOG(INFO) << "Successfully start CacheNode{id=" << FLAGS_id
            << " shards=" << ShardCount()
            << " rdma=" << (FLAGS_rdma ? "on" : "off")
            << " listen_port=" << FLAGS_listen_port << "}";
  return Status::OK();
}

void CacheNode::Shutdown() {
  if (!running_) {
    return;
  }

  LOG(INFO) << "CacheNode is shutting down...";

  heartbeat_->Shutdown();
  membership_->Shutdown();
  server_->Shutdown();
  block_cache_->Shutdown();

  running_ = false;
  LOG(INFO) << "Successfully shutdown CacheNode";
}

Status CacheNode::StartServer() {
  {
    ServerOption option;
    option.buffer_pool_bytes = FLAGS_buffer_pool_mb << 20;
    option.rdma.enabled = FLAGS_rdma;
    option.rdma.device = FLAGS_rdma_device;
    option.rdma.idle_timeout_ns =
        uint64_t{FLAGS_rdma_idle_timeout_s} * 1'000'000'000;
    server_ = std::make_unique<Server>(option);
    server_->AddService<CacheService>(block_cache_.get());
  }

  {
    BrpcTransport::Option option;
    option.listen_ip = FLAGS_bind_ip;
    option.listen_port = static_cast<uint16_t>(FLAGS_listen_port);
    server_->AddTransport(std::make_unique<BrpcTransport>(option));
  }

  Status status = server_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to start server: " << status.ToString();
  }
  return status;
}

void CacheNode::RunUntilAskedToQuit() { server_->RunUntilAskedToQuit(); }

}  // namespace blockcache
}  // namespace dingofs
