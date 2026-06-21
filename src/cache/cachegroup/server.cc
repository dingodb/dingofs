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
 * Created Date: 2025-01-13
 * Author: Jingli Chen (Wine93)
 */

#include "cache/cachegroup/server.h"

#include <atomic>
#include <memory>

#include "cache/cachegroup/service.h"
#include "cache/infiniband/connection.h"
#include "cache/infiniband/infiniband.h"
#include "cache/infiniband/server.h"
#include "cache/infiniband/slab_pool.h"
#include "cache/iutil/string_util.h"
#include "common/options/cache.h"
#include "fmt/format.h"

namespace brpc {
DECLARE_bool(graceful_quit_on_sigterm);
}  // namespace brpc

namespace dingofs {
namespace cache {

DEFINE_string(listen_ip, "", "ip address to listen on for this cache node");
DEFINE_validator(listen_ip, iutil::StringValidator);

DEFINE_uint32(listen_port, 9300, "port to listen on for this cache node");

DEFINE_bool(public_address, true,
            "listen on 0.0.0.0 instead of --listen_ip so the cache node is "
            "reachable by remote clients");

Server::Server()
    : running_(false),
      node_(std::make_unique<CacheNode>()),
      rdma_service_(std::make_unique<BlockCacheServiceImpl>(ServiceType::kRDMA,
                                                            node_.get())),
      brpc_service_(std::make_unique<BlockCacheServiceImpl>(ServiceType::kBRPC,
                                                            node_.get())),
      rdma_server_(std::make_unique<infiniband::Server>()),
      brpc_server_(std::make_unique<brpc::Server>()) {}

Status Server::Start() {
  if (running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "Server already started";
    return Status::OK();
  }

  LOG(INFO) << "Server is starting...";

  InstallSignal();

  if (FLAGS_use_rdma) {
    auto status = StartRDMAServer();
    if (!status.ok()) {
      LOG(ERROR) << "Fail to start RDMA server: " << status.ToString();
      return status;
    }
  }

  std::string listen_ip = FLAGS_public_address ? "0.0.0.0" : FLAGS_listen_ip;
  auto status = StartBrpcServer(listen_ip, FLAGS_listen_port);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to start rpc server at " << FLAGS_listen_ip << ":"
               << FLAGS_listen_port;
    return status;
  }

  status = node_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to start CacheNode";
    return status;
  }

  running_.store(true, std::memory_order_relaxed);

  LOG(INFO) << "Cache node server is up, address=" << listen_ip << ":"
            << FLAGS_listen_port;

  std::cout << "\ndingo-cache is listening on " << listen_ip << ":"
            << FLAGS_listen_port << "\n";

  // Run until asked to quit
  brpc::FLAGS_graceful_quit_on_sigterm = true;
  brpc_server_->RunUntilAskedToQuit();

  return Status::OK();
}

Status Server::Shutdown() {
  if (!running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "Server already shutdown";
    return Status::OK();
  }

  LOG(INFO) << "Server is shutting down...";

  ShutdownRDMAServer();
  ShutdownBrpcServer();

  auto status = node_->Shutdown();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to shutdown CacheNode";
    return status;
  }

  running_.store(false, std::memory_order_relaxed);
  LOG(INFO) << "Server is shutdown";
  return status;
}

void Server::InstallSignal() { CHECK(SIG_ERR != signal(SIGPIPE, SIG_IGN)); }

Status Server::StartRDMAServer() {
  infiniband::EndPoint endpoint{
      FLAGS_cache_rdma_device,
      static_cast<uint8_t>(FLAGS_cache_rdma_port_num),
  };
  infiniband::ServerOptions options{.brpc_server = brpc_server_.get()};
  auto status = rdma_server_->Start(endpoint, &options);
  if (!status.ok()) {
    return status;
  }

  status = rdma_server_->AddService(rdma_service_.get());
  if (!status.ok()) {
    return status;
  }

  infiniband::Infiniband::Context ctx;
  status = infiniband::Infiniband::Init(
      FLAGS_cache_rdma_device, static_cast<uint8_t>(FLAGS_cache_rdma_port_num), &ctx);
  if (!status.ok()) {
    return status;
  }

  LOG(INFO) << "RDMA cache service is up on device=" << FLAGS_cache_rdma_device
            << " port_num=" << FLAGS_cache_rdma_port_num;
  return Status::OK();
}

Status Server::StartBrpcServer(const std::string& listen_ip,
                               uint32_t listen_port) {
  butil::EndPoint ep;
  int rc = butil::str2endpoint(listen_ip.c_str(), listen_port, &ep);
  if (rc != 0) {
    LOG(ERROR) << "Fail to str2endpoint(" << listen_ip << "," << listen_port
               << ")";
    return Status::Internal("str2endpoint() failed");
  }

  rc = brpc_server_->AddService(brpc_service_.get(),
                                brpc::SERVER_DOESNT_OWN_SERVICE);
  if (rc != 0) {
    LOG(ERROR) << "Fail to add BlockCacheService to brpc server";
    return Status::Internal("add service failed");
  }

  brpc::ServerOptions options;
  options.ignore_eovercrowded = true;
  rc = brpc_server_->Start(ep, &options);
  if (rc != 0) {
    LOG(ERROR) << "Fail to start brpc server";
    return Status::Internal("start brpc server failed");
  }
  return Status::OK();
}

}  // namespace cache
}  // namespace dingofs
