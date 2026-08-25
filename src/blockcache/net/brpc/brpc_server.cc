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

#include "blockcache/net/brpc/brpc_server.h"

#include <brpc/reloadable_flags.h>
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <string>
#include <utility>

namespace dingofs {
namespace blockcache {

DEFINE_int32(brpc_idle_timeout_s, -1,
             "seconds an idle connection is kept; -1 never reaps");
DEFINE_validator(brpc_idle_timeout_s, brpc::PassValidate);

DEFINE_int32(brpc_max_concurrency, 0,
             "requests in flight brpc admits; 0 is unlimited");
DEFINE_validator(brpc_max_concurrency, brpc::NonNegativeInteger);

DEFINE_bool(brpc_reply_on_bthread, false,
            "run brpc's done on a bthread instead of on the owning shard");

BrpcServer::BrpcServer(Option option) : option_(std::move(option)) {}

BrpcServer::~BrpcServer() { Shutdown(); }

void BrpcServer::AddService(
    std::unique_ptr<google::protobuf::Service> service) {
  CHECK(!started_) << "AddService after Start()";
  services_.push_back(std::move(service));
}

Status BrpcServer::Start() {
  CHECK(!started_) << "BrpcServer already started";

  server_ = std::make_unique<::brpc::Server>();
  for (const auto& service : services_) {
    if (server_->AddService(service.get(), ::brpc::SERVER_DOESNT_OWN_SERVICE) !=
        0) {
      return Status::Internal("Fail to add the service " +
                              service->GetDescriptor()->full_name());
    }
  }

  butil::EndPoint listen_addr;
  if (butil::str2endpoint(option_.listen_ip.c_str(), option_.listen_port,
                          &listen_addr) != 0) {
    return Status::InvalidParam("Fail to parse the listen address: " +
                                option_.listen_ip);
  }

  ::brpc::ServerOptions options;
  options.idle_timeout_sec = FLAGS_brpc_idle_timeout_s;
  options.max_concurrency = FLAGS_brpc_max_concurrency;
  if (server_->Start(listen_addr, &options) != 0) {
    return Status::Internal("Fail to start the brpc server");
  }

  started_ = true;
  return Status::OK();
}

void BrpcServer::Shutdown() {
  if (!started_) {
    return;
  }
  started_ = false;

  server_->Stop(0);
  DrainInflight(inflight_);
  server_->Join();
  server_.reset();
}

bool BrpcServer::reply_on_bthread() { return FLAGS_brpc_reply_on_bthread; }

}  // namespace blockcache
}  // namespace dingofs
