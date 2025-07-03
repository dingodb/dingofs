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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_V2_RPC_H_
#define DINGOFS_SRC_CLIENT_VFS_META_V2_RPC_H_

#include <butil/endpoint.h>
#include <fmt/format.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <json/config.h>

#include <cstdint>
#include <map>
#include <memory>
#include <string>

#include "brpc/channel.h"
#include "brpc/controller.h"
#include "common/status.h"
#include "dingofs/error.pb.h"
#include "dingofs/mdsv2.pb.h"
#include "fmt/core.h"
#include "mdsv2/common/helper.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

DECLARE_uint32(rpc_timeout_ms);
DECLARE_int32(rpc_retry_times);

class RPC;
using RPCPtr = std::shared_ptr<RPC>;

using EndPoint = butil::EndPoint;

inline EndPoint StrToEndpoint(const std::string& ip, int port) {
  EndPoint endpoint;
  butil::str2endpoint(ip.c_str(), port, &endpoint);

  return endpoint;
}

inline std::string EndPointToStr(const EndPoint& endpoint) {
  return butil::endpoint2str(endpoint).c_str();
}

inline std::string TakeIp(const EndPoint& endpoint) {
  return std::string(butil::ip2str(endpoint.ip).c_str());
}

class RPC {
 public:
  RPC(const std::string& addr);
  RPC(const std::string& ip, int port);
  RPC(const EndPoint& endpoint);
  ~RPC() = default;

  static RPCPtr New(const std::string& addr) {
    return std::make_shared<RPC>(addr);
  }
  static RPCPtr New(const std::string& ip, int port) {
    return std::make_shared<RPC>(ip, port);
  }
  static RPCPtr New(const EndPoint& endpoint) {
    return std::make_shared<RPC>(endpoint);
  }

  bool Init();
  void Destory();

  bool AddEndpoint(const std::string& ip, int port, bool is_default = false);
  void DeleteEndpoint(const std::string& ip, int port);

  template <typename Request, typename Response>
  Status SendRequest(const std::string& service_name,
                     const std::string& api_name, const Request& request,
                     Response& response) {
    return SendRequest(default_endpoint_, service_name, api_name, request,
                       response);
  }

  template <typename Request, typename Response>
  Status SendRequest(const EndPoint& endpoint, const std::string& service_name,
                     const std::string& api_name, const Request& request,
                     Response& response);

 private:
  using Channel = brpc::Channel;
  using ChannelPtr = std::shared_ptr<Channel>;

  ChannelPtr NewChannel(const EndPoint& endpoint);
  Channel* GetChannel(const EndPoint& endpoint);
  void DeleteChannel(const EndPoint& endpoint);

  utils::RWLock lock_;
  std::map<EndPoint, ChannelPtr> channels_;
  EndPoint default_endpoint_;
};

inline Status TransformError(const pb::error::Error& error) {
  switch (error.errcode()) {
    case pb::error::EQUOTA_EXCEED:
      return Status::NoSpace(error.errcode(), error.errmsg());

    case pb::error::ENOT_SUPPORT:
    case pb::error::EQUOTA_ILLEGAL:
      return Status::NotSupport(error.errcode(), error.errmsg());

    case pb::error::ENOT_EMPTY:
      return Status::NotEmpty(error.errcode(), error.errmsg());

    case pb::error::ENOT_FOUND:
      return Status::NotExist(error.errcode(), error.errmsg());

    default:
      return Status::Internal(error.errcode(), error.errmsg());
  }
}

template <typename Request, typename Response>
Status RPC::SendRequest(const EndPoint& endpoint,
                        const std::string& service_name,
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
  CHECK(channel != nullptr)
      << fmt::format("[rpc][{}] channel is null.", EndPointToStr(endpoint));

  int retry_count = 0;
  do {
    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpc_timeout_ms);
    cntl.set_log_id(butil::fast_rand());

    uint64_t start_us = mdsv2::Helper::TimestampUs();
    channel->CallMethod(method, &cntl, &request, &response, nullptr);
    uint64_t elapsed_us = mdsv2::Helper::TimestampUs() - start_us;
    if (cntl.Failed()) {
      LOG(ERROR) << fmt::format(
          "[rpc][{}][{}][{}us] fail, {} {} {} request({}).",
          EndPointToStr(endpoint), api_name, elapsed_us, cntl.log_id(),
          cntl.ErrorCode(), cntl.ErrorText(), request.ShortDebugString());
      DeleteChannel(endpoint);
      return Status::NetError(cntl.ErrorCode(), cntl.ErrorText());
    }

    if (response.error().errcode() == pb::error::OK) {
      LOG(INFO) << fmt::format(
          "[rpc][{}][{}][{}us] success, request({}) response({}).",
          EndPointToStr(endpoint), api_name, elapsed_us,
          request.ShortDebugString(), response.ShortDebugString());
      return Status();
    }

    ++retry_count;

    if (response.error().errcode() != pb::error::ENOT_FOUND) {
      LOG(ERROR) << fmt::format(
          "[rpc][{}][{}][{}us] fail, request({}) retry_count({}) error({} {}).",
          EndPointToStr(endpoint), api_name, elapsed_us,
          request.ShortDebugString(), retry_count,
          pb::error::Errno_Name(response.error().errcode()),
          response.error().errmsg());
    }

    // the errno of need retry
    if (response.error().errcode() != pb::error::EINTERNAL) {
      break;
    }

  } while (retry_count < FLAGS_rpc_retry_times);

  return TransformError(response.error());
}

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_V2_RPC_H_