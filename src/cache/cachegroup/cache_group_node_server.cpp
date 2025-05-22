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
#include <glog/logging.h>

#include <cstdint>
#include <memory>
#include <string>

#include "cache/cachegroup/cache_group_node.h"
#include "cache/utils/access_log.h"
#include "common/status.h"
#include "blockaccess/block_access_log.h"

namespace brpc {
DECLARE_bool(graceful_quit_on_sigterm);
}  // namespace brpc

namespace dingofs {
namespace cache {
namespace cachegroup {

using dingofs::cache::utils::InitCacheAccessLog;
using dingofs::blockaccess::InitBlockAccessLog;

CacheGroupNodeServerImpl::CacheGroupNodeServerImpl(AppOption option)
    : option_(option),
      node_(std::make_shared<CacheGroupNodeImpl>(
          option.cache_group_node_option())),
      service_(std::make_unique<CacheGroupNodeServiceImpl>(node_)),
      server_(std::make_unique<::brpc::Server>()) {}

void CacheGroupNodeServerImpl::InstallSignal() {
  CHECK(SIG_ERR != signal(SIGPIPE, SIG_IGN));
}

Status CacheGroupNodeServerImpl::InitLogger() {
  // glog
  const auto& option = option_.global_option();
  FLAGS_log_dir = option.log_dir();
  FLAGS_v = option.log_level();
  FLAGS_logbufsecs = 0;
  FLAGS_max_log_size = 80;
  FLAGS_stop_logging_if_full_disk = true;

  static const std::string program_name = "dingo-cache";
  google::InitGoogleLogging(program_name.c_str());
  LOG(INFO) << "init logger success, log_dir = " << FLAGS_log_dir;

  // access logging
  if (option.block_cache_access_logging() &&
      !InitCacheAccessLog(FLAGS_log_dir)) {
    return Status::Internal("Init cache access logging failed");
  } else if (option.s3_access_logging() && !InitBlockAccessLog(FLAGS_log_dir)) {
    return Status::Internal("Init s3 access logging failed");
  }

  return Status::OK();
}

Status CacheGroupNodeServerImpl::Init() {
  InstallSignal();
  return InitLogger();
}

Status CacheGroupNodeServerImpl::StartRpcServer(const std::string& listen_ip,
                                                uint32_t listen_port) {
  butil::EndPoint ep;
  int rc = butil::str2endpoint(listen_ip.c_str(), listen_port, &ep);
  if (rc != 0) {
    LOG(ERROR) << "str2endpoint(" << listen_ip << "," << listen_port
               << ") failed, rc = " << rc;
    return Status::Internal("str2endpoint() failed");
  }

  rc = server_->AddService(service_.get(), brpc::SERVER_DOESNT_OWN_SERVICE);
  if (rc != 0) {
    LOG(ERROR) << "Add block cache service to server failed, rc = " << rc;
    return Status::Internal("Add service failed");
  }

  brpc::ServerOptions options;
  rc = server_->Start(ep, &options);
  if (rc != 0) {
    LOG(ERROR) << "Start brpc server failed, rc = " << rc;
    return Status::Internal("Start server failed");
  }
  return Status::OK();
}

Status CacheGroupNodeServerImpl::Run() {
  auto status = node_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Start cache group node failed, status = "
               << status.ToString();
    return status;
  }

  std::string listen_ip = node_->GetListenIp();
  uint32_t listen_port = node_->GetListenPort();
  status = StartRpcServer(listen_ip, listen_port);
  if (!status.ok()) {
    LOG(ERROR) << "Start cache group node server on(" << listen_ip << ":"
               << listen_port << ") failed.";
    return status;
  }

  LOG(INFO) << "Start cache group node server on(" << listen_ip << ":"
            << listen_port << ") success.";

  brpc::FLAGS_graceful_quit_on_sigterm = true;
  server_->RunUntilAskedToQuit();
  return Status::OK();
}

void CacheGroupNodeServerImpl::Shutdown() { brpc::AskToQuit(); }

}  // namespace cachegroup
}  // namespace cache
}  // namespace dingofs
