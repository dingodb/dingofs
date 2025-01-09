// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGOFS_SRC_CLIENT_FILESYSTEMV2_RPC_H_
#define DINGOFS_SRC_CLIENT_FILESYSTEMV2_RPC_H_

#include <gflags/gflags_declare.h>
#include <json/config.h>

#include <map>
#include <memory>
#include <string>

#include "brpc/channel.h"
#include "brpc/controller.h"
#include "bthread/types.h"
#include "dingofs/mdsv2.pb.h"
#include "fmt/core.h"
#include "mdsv2/common/status.h"

namespace dingofs {
namespace client {
namespace filesystem {

DECLARE_int32(rpc_retry_times);

class EndPoint {
 public:
  EndPoint() = default;
  EndPoint(const std::string& ip, int port) : ip_(ip), port_(port) {}
  ~EndPoint() = default;

  bool operator<(const EndPoint& other) const {
    return other.ip_ < ip_ || (other.ip_ == ip_ && other.port_ < port_);
  }

  const std::string& GetIp() const { return ip_; }
  void SetIp(const std::string& ip) { ip_ = ip; }

  int GetPort() const { return port_; }
  void SetPort(int port) { port_ = port; }

 private:
  std::string ip_;
  int port_;
};

class RPC {
 public:
  RPC();
  ~RPC();

  bool Init();
  void Destory();

  template <typename Request, typename Response>
  Status SendRequest(const std::string& service_name,
                     const std::string& api_name, const Request& request,
                     Response& response) {
    return SendRequest(EndPoint(), service_name, api_name, request, response);
  }

  template <typename Request, typename Response>
  Status SendRequest(EndPoint endpoint, const std::string& service_name,
                     const std::string& api_name, const Request& request,
                     Response& response);

 private:
  using Channel = brpc::Channel;
  using ChannelPtr = std::unique_ptr<Channel>;

  Channel* GetChannel(EndPoint endpoint);

  bthread_mutex_t mutex_;
  std::map<EndPoint, ChannelPtr> channels_;
};

template <typename Request, typename Response>
Status RPC::SendRequest(EndPoint endpoint, const std::string& service_name,
                        const std::string& api_name, const Request& request,
                        Response& response) {
  const google::protobuf::MethodDescriptor* method = nullptr;

  if (service_name == "MDSService") {
    method = dingofs::pb::mdsv2::MDSService::descriptor()->FindMethodByName(
        api_name);
  } else {
    LOG(FATAL) << "Unknown service name: " << service_name;
  }

  if (method == nullptr) {
    LOG(FATAL) << "Unknown api name: " << api_name;
  }

  auto* channel = GetChannel(endpoint);

  int retry_count = 0;
  do {
    brpc::Controller cntl;
    cntl.set_timeout_ms(1000);
    cntl.set_log_id(butil::fast_rand());

    channel->CallMethod(method, &cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
      LOG(ERROR) << fmt::format("{} response failed, {} {} {}", api_name,
                                cntl.log_id(), cntl.ErrorCode(),
                                cntl.ErrorText());
      return butil::Status(cntl.ErrorCode(), cntl.ErrorText());
    }

    if (response.error().errcode() == pb::error::OK) {
      return Status();
    }

  } while (retry_count < FLAGS_rpc_retry_times);

  LOG(ERROR) << fmt::format("{} response failed, error: {} {}", api_name,
                            pb::error::Errno_Name(response.error().errcode()),
                            response.error().errmsg());

  return butil::Status(response.error().errcode(), response.error().errmsg());
}

using RPCPtr = std::shared_ptr<RPC>;

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_FILESYSTEMV2_RPC_H_