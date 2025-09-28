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

#include <brpc/errno.pb.h>
#include <bthread/bthread.h>
#include <butil/endpoint.h>
#include <fmt/format.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <json/config.h>

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "brpc/channel.h"
#include "brpc/controller.h"
#include "common/status.h"
#include "dingofs/error.pb.h"
#include "dingofs/mds.pb.h"
#include "fmt/core.h"
#include "fmt/ranges.h"
#include "json/value.h"
#include "mds/common/helper.h"
#include "mds/common/synchronization.h"
#include "options/client/option.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

class RPC;
using RPCPtr = std::shared_ptr<RPC>;

using EndPoint = butil::EndPoint;

inline bool IsInvalidEndpoint(const EndPoint& endpoint) {
  return endpoint.ip == butil::IP_NONE || endpoint.port <= 0;
}

inline uint32_t CalWaitTimeUs(int retry) {
  // exponential backoff
  return mds::Helper::GenerateRealRandomInteger(50000, 100000) * (1 << retry);
}

inline bool IsRetry(int& retry, int max_retry) {
  if (++retry <= max_retry) {
    bthread_usleep(CalWaitTimeUs(retry));
    return true;
  }
  return false;
}

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

struct SendRequestOption {
  SendRequestOption()
      : timeout_ms(FLAGS_client_vfs_rpc_timeout_ms),
        max_retry(FLAGS_client_vfs_rpc_retry_times) {}

  int64_t timeout_ms;
  int max_retry;
};

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

  static bool CheckMdsAlive(const std::string& addr);

  bool AddEndpoint(const std::string& ip, int port);
  void DeleteEndpoint(const std::string& ip, int port);

  void AddFallbackEndpoint(const EndPoint& endpoint);

  template <typename Request, typename Response>
  Status SendRequest(const std::string& service_name,
                     const std::string& api_name, const Request& request,
                     Response& response) {
    return SendRequest(RandomlyPickupEndPoint(), service_name, api_name,
                       request, response);
  }

  std::string GetInitEndPoint() { return EndPointToStr(init_endpoint_); }

  template <typename Request, typename Response>
  Status SendRequest(const EndPoint& endpoint, const std::string& service_name,
                     const std::string& api_name, const Request& request,
                     Response& response,
                     SendRequestOption option = SendRequestOption());

  bool Dump(Json::Value& value);

 private:
  using Channel = brpc::Channel;
  using ChannelSPtr = std::shared_ptr<Channel>;

  ChannelSPtr NewChannel(const EndPoint& endpoint);
  ChannelSPtr GetOrAddChannel(const EndPoint& endpoint);
  void DeleteChannel(const EndPoint& endpoint);
  EndPoint RandomlyPickupEndPoint();

  void IncDoingReqCount() {
    doing_req_count_.fetch_add(1, std::memory_order_relaxed);
  }
  void DecDoingReqCount() {
    doing_req_count_.fetch_sub(1, std::memory_order_relaxed);
  }
  uint64_t DoingReqCount() {
    return doing_req_count_.load(std::memory_order_relaxed);
  }

  utils::RWLock lock_;
  EndPoint init_endpoint_;
  std::map<EndPoint, ChannelSPtr> channels_;
  std::set<EndPoint> fallback_endpoints_;

  std::atomic<uint64_t> doing_req_count_{0};
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

    case pb::error::ENOT_CAN_CONNECTED:
      return Status::NetError(error.errcode(), error.errmsg());

    default:
      return Status::Internal(error.errcode(), error.errmsg());
  }
}

// print ReadSliceResponse
inline std::string DescribeReadSliceResponse(
    pb::mds::ReadSliceResponse& response) {
  std::ostringstream oss;
  oss << response.info().ShortDebugString() << " chunks[";
  for (const auto& chunk : response.chunks()) {
    std::vector<uint64_t> slice_ids;
    slice_ids.reserve(chunk.slices().size());
    for (const auto& slice : chunk.slices()) {
      slice_ids.push_back(slice.id());
    }
    oss << fmt::format("({},{} slice_ids{}),", chunk.index(), chunk.version(),
                       slice_ids);
  }

  oss << "]";
  return oss.str();
}

template <typename Request, typename Response>
Status RPC::SendRequest(const EndPoint& endpoint,
                        const std::string& service_name,
                        const std::string& api_name, const Request& request,
                        Response& response, SendRequestOption option) {
  IncDoingReqCount();
  mds::DEFER(DecDoingReqCount());

  const google::protobuf::MethodDescriptor* method = nullptr;

  CHECK(service_name == "MDSService")
      << "[meta.rpc] unknown service name: " << service_name;

  method =
      dingofs::pb::mds::MDSService::descriptor()->FindMethodByName(api_name);
  CHECK(method != nullptr) << "[meta.rpc] unknown api name: " << api_name;

  if (IsInvalidEndpoint(endpoint)) {
    LOG(ERROR) << fmt::format("[meta.rpc][{}] endpoint is invalid.",
                              EndPointToStr(endpoint));
    return Status::Internal("endpoint is invalid");
  }

  auto channel = GetOrAddChannel(endpoint);
  CHECK(channel != nullptr) << fmt::format("[meta.rpc][{}] channel is null.",
                                           EndPointToStr(endpoint));

  int retry = 0;
  do {
    brpc::Controller cntl;
    cntl.set_timeout_ms(option.timeout_ms);
    cntl.set_log_id(butil::fast_rand());

    uint64_t start_us = mds::Helper::TimestampUs();
    channel->CallMethod(method, &cntl, &request, &response, nullptr);
    uint64_t elapsed_us = mds::Helper::TimestampUs() - start_us;
    if (cntl.Failed()) {
      LOG(ERROR) << fmt::format(
          "[meta.rpc][{}][{}][{}us] fail, {} retry({}) {} request({}) "
          "doing({}).",
          EndPointToStr(endpoint), api_name, elapsed_us, cntl.log_id(), retry,
          cntl.ErrorText(), request.ShortDebugString(), DoingReqCount());

      response.mutable_error()->set_errcode(pb::error::EINTERNAL);
      response.mutable_error()->set_errmsg(
          fmt::format("{} {}", cntl.ErrorCode(), cntl.ErrorText()));

      // if the error is timeout, we can retry
      if (cntl.ErrorCode() == brpc::ERPCTIMEDOUT) continue;
      DeleteChannel(endpoint);
      if (cntl.ErrorCode() == EHOSTDOWN) {
        response.mutable_error()->set_errcode(pb::error::ENOT_CAN_CONNECTED);
        ++retry;
        continue;
      }

      return Status::NetError(cntl.ErrorCode(), cntl.ErrorText());
    }

    if (response.error().errcode() == pb::error::OK) {
      if constexpr (!std::is_same_v<Response, pb::mds::ReadSliceResponse>) {
        LOG(INFO) << fmt::format(
            "[meta.rpc][{}][{}][{}us] success, retry({}) request({}) "
            "response({}) doing({}).",
            EndPointToStr(endpoint), api_name, elapsed_us, retry,
            request.ShortDebugString(), response.ShortDebugString(),
            DoingReqCount());
      } else {
        LOG(INFO) << fmt::format(
            "[meta.rpc][{}][{}][{}us] success, retry({}) request({}) "
            "response({}) doing({}).",
            EndPointToStr(endpoint), api_name, elapsed_us, retry,
            request.ShortDebugString(), DescribeReadSliceResponse(response),
            DoingReqCount());
      }
      return Status::OK();
    }

    if (response.error().errcode() != pb::error::ENOT_FOUND) {
      LOG(ERROR) << fmt::format(
          "[meta.rpc][{}][{}][{}us] fail, retry({}) request({}) "
          "doing({}) error({} {}).",
          EndPointToStr(endpoint), api_name, elapsed_us, retry,
          request.ShortDebugString(), DoingReqCount(),
          pb::error::Errno_Name(response.error().errcode()),
          response.error().errmsg());
    }

    // the errno of need retry
    if (response.error().errcode() != pb::error::EINTERNAL &&
        response.error().errcode() != pb::error::EBACKEND_STORE) {
      break;
    }

  } while (IsRetry(retry, option.max_retry));

  return TransformError(response.error());
}

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_V2_RPC_H_
