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

#include <brpc/server.h>

#include <cstdint>
#include <memory>
#include <string>

namespace brpc {
DECLARE_bool(graceful_quit_on_sigterm);
}  // namespace brpc

namespace dingofs {
namespace cache {
namespace cachegroup {

CacheGroupNodeServerImpl::CacheGroupNodeServerImpl(
    std::shared_ptr<CacheGroupNode> node)
    : node_(node),
      service_(std::make_unique<CacheGroupNodeServiceImpl>(node)),
      server_(std::make_unique<::brpc::Server>()) {}

bool CacheGroupNodeServerImpl::StartRpcServer(const std::string& listen_ip,
                                              uint32_t listen_port) {
  butil::EndPoint ep;
  int rc = butil::str2endpoint(listen_ip.c_str(), listen_port, &ep);
  if (rc != 0) {
    LOG(ERROR) << "str2endpoint(" << listen_ip << "," << listen_port
               << ") failed, rc = " << rc;
    return false;
  }

  rc = server_->AddService(service_.get(), brpc::SERVER_DOESNT_OWN_SERVICE);
  if (rc != 0) {
    LOG(ERROR) << "Add block cache service to server failed, rc = " << rc;
    return false;
  }

  brpc::ServerOptions options;
  rc = server_->Start(ep, &options);
  if (rc != 0) {
    LOG(ERROR) << "Start brpc server failed, rc = " << rc;
    return false;
  }
  return true;
}

bool CacheGroupNodeServerImpl::Serve() {
  std::string listen_ip = node_->GetListenIp();
  uint32_t listen_port = node_->GetListenPort();
  auto succ = node_->Start();
  if (succ) {
    succ = StartRpcServer(listen_ip, listen_port);
  }

  if (!succ) {
    LOG(ERROR) << "Start cache group node server on(" << listen_ip << ":"
               << listen_port << ") failed.";
    return false;
  }

  LOG(INFO) << "Start cache group node server on(" << listen_ip << ":"
            << listen_port << ") success.";

  brpc::FLAGS_graceful_quit_on_sigterm = true;
  server_.RunUntilAskedToQuit();
  return true;
}

bool CacheGroupNodeServerImpl::Shutdown() { brpc::AskToQuit(); }

}  // namespace cachegroup
}  // namespace cache
}  // namespace dingofs
