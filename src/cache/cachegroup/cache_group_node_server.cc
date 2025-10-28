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

#include "cache/cachegroup/cache_group_node_server.h"

#include "cache/cachegroup/cache_group_node.h"
#include "cache/cachegroup/cache_group_node_service.h"
#include "cache/common/macro.h"

namespace brpc {
DECLARE_bool(graceful_quit_on_sigterm);
}  // namespace brpc

namespace dingofs {
namespace cache {

CacheGroupNodeServerImpl::CacheGroupNodeServerImpl()
    : running_(false),
      node_(std::make_shared<CacheGroupNodeImpl>()),
      service_(std::make_unique<CacheGroupNodeServiceImpl>(node_)),
      server_(std::make_unique<::brpc::Server>()) {}

Status CacheGroupNodeServerImpl::Start() {
  CHECK_NOTNULL(node_);
  CHECK_NOTNULL(service_);
  CHECK_NOTNULL(server_);

  if (running_) {
    return Status::OK();
  }

  LOG(INFO) << "Cache group node server is starting...";

  // Init signal
  InstallSignal();

  // Start brpc server
  auto listen_ip = FLAGS_listen_ip;
  auto listen_port = FLAGS_listen_port;
  auto status = StartRpcServer(listen_ip, listen_port);
  if (!status.ok()) {
    LOG(ERROR) << "Start cache group node server on addresss (" << listen_ip
               << ":" << listen_port << ") failed: " << status.ToString();
    return status;
  }

  // Start cache group node
  status = node_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Start cache group node failed: " << status.ToString();
    return status;
  }

  running_ = true;

  // Run until asked to quit
  LOG(INFO) << "Cache group node server is up: address = " << listen_ip << ":"
            << listen_port;

  CHECK_RUNNING("Cache group node server");

  brpc::FLAGS_graceful_quit_on_sigterm = true;
  server_->RunUntilAskedToQuit();

  return Status::OK();
}

Status CacheGroupNodeServerImpl::Shutdown() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }

  LOG(INFO) << "Cache group node server is shutting down...";

  brpc::AskToQuit();
  auto status = node_->Shutdown();
  if (!status.ok()) {
    LOG(ERROR) << "Cache group node server shutdown failed: "
               << status.ToString();
    return status;
  }

  LOG(INFO) << "Cache group node server is down.";

  CHECK_DOWN("Cache group node server");
  return status;
}

void CacheGroupNodeServerImpl::InstallSignal() {
  CHECK(SIG_ERR != signal(SIGPIPE, SIG_IGN));
}

Status CacheGroupNodeServerImpl::StartRpcServer(const std::string& listen_ip,
                                                uint32_t listen_port) {
  butil::EndPoint ep;
  int rc = butil::str2endpoint(listen_ip.c_str(), listen_port, &ep);
  if (rc != 0) {
    LOG(ERROR) << "str2endpoint(" << listen_ip << "," << listen_port
               << ") failed: rc = " << rc;
    return Status::Internal("str2endpoint() failed");
  }

  rc = server_->AddService(service_.get(), brpc::SERVER_DOESNT_OWN_SERVICE);
  if (rc != 0) {
    LOG(ERROR) << "Add block cache service to server failed: rc = " << rc;
    return Status::Internal("add service failed");
  }

  brpc::ServerOptions options;
  rc = server_->Start(ep, &options);
  if (rc != 0) {
    LOG(ERROR) << "Start brpc server failed: rc = " << rc;
    return Status::Internal("start server failed");
  }
  return Status::OK();
}

}  // namespace cache
}  // namespace dingofs
