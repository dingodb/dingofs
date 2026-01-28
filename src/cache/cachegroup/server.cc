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

#include "cache/cachegroup/service.h"
#include "cache/iutil/string_util.h"
#include "common/options/cache.h"
#include "fmt/format.h"

namespace brpc {
DECLARE_bool(graceful_quit_on_sigterm);
}  // namespace brpc

namespace dingofs {
namespace cache {

DEFINE_bool(wide_access, true, "whether to enable wide access listen address");

DEFINE_string(listen_ip, "", "ip address to listen on for this cache node");
DEFINE_validator(listen_ip, iutil::StringValidator);

DEFINE_uint32(listen_port, 9300, "port to listen on for this cache node");

static void PrintReadyInfo(const std::string& addr) {
  std::cout << "\n";
  std::cout << "dingo-cache is listening on " << addr;
  std::cout << "\n";
  std::cout.flush();
}

Server::Server()
    : running_(false),
      node_(std::make_shared<CacheNode>()),
      service_(std::make_unique<BlockCacheServiceImpl>(node_)),
      server_(std::make_unique<::brpc::Server>()) {}

Status Server::Start() {
  if (running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "Server already started";
    return Status::OK();
  }

  LOG(INFO) << "Server is starting...";

  InstallSignal();

  std::string listen_ip = FLAGS_wide_access ? "0.0.0.0" : FLAGS_listen_ip;
  auto status = StartRpcServer(FLAGS_listen_ip, FLAGS_listen_port);
  if (!status.ok()) {
    LOG(FATAL) << "Fail to start rpc server at " << FLAGS_listen_ip << ":"
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

  PrintReadyInfo(fmt::format("{}:{}", listen_ip, FLAGS_listen_port));

  // Run until asked to quit
  brpc::FLAGS_graceful_quit_on_sigterm = true;
  server_->RunUntilAskedToQuit();

  return Status::OK();
}

Status Server::Shutdown() {
  if (!running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "Server already shutdown";
    return Status::OK();
  }

  LOG(INFO) << "Server is shutting down...";

  brpc::AskToQuit();
  auto status = node_->Shutdown();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to shutdown CacheNode";
    return status;
  }

  running_.store(false, std::memory_order_relaxed);
  LOG(INFO) << "Server is down";
  return status;
}

void Server::InstallSignal() { CHECK(SIG_ERR != signal(SIGPIPE, SIG_IGN)); }

Status Server::StartRpcServer(const std::string& listen_ip,
                              uint32_t listen_port) {
  butil::EndPoint ep;
  int rc = butil::str2endpoint(listen_ip.c_str(), listen_port, &ep);
  if (rc != 0) {
    LOG(ERROR) << "Fail to str2endpoint(" << listen_ip << "," << listen_port
               << ")";
    return Status::Internal("str2endpoint() failed");
  }

  rc = server_->AddService(service_.get(), brpc::SERVER_DOESNT_OWN_SERVICE);
  if (rc != 0) {
    LOG(ERROR) << "Fail to add BlockCacheService to brpc server";
    return Status::Internal("add service failed");
  }

  brpc::ServerOptions options;
  options.ignore_eovercrowded = true;
  rc = server_->Start(ep, &options);
  if (rc != 0) {
    LOG(ERROR) << "Fail to start brpc server";
    return Status::Internal("start brpc server failed");
  }
  return Status::OK();
}

}  // namespace cache
}  // namespace dingofs
