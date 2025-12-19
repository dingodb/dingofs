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

#include "client/vfs/metasystem/mds/rpc.h"

#include <string>

#include "brpc/channel.h"
#include "butil/endpoint.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "utils/concurrent/concurrent.h"
#include "utils/time.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

const int32_t kConnectTimeoutMs = 200;  // milliseconds

RPC::RPC(const std::string& addr) {
  EndPoint endpoint;
  butil::str2endpoint(addr.c_str(), &endpoint);
  init_endpoint_ = endpoint;
}

RPC::RPC(const std::string& ip, int port) {
  EndPoint endpoint;
  butil::str2endpoint(ip.c_str(), port, &endpoint);
  init_endpoint_ = endpoint;
}

RPC::RPC(const EndPoint& endpoint) : init_endpoint_(endpoint) {}

bool RPC::Init() {
  utils::WriteLockGuard lk(lock_);

  ChannelSPtr channel = NewChannel(init_endpoint_);
  if (channel == nullptr) {
    return false;
  }
  channels_.insert(std::make_pair(init_endpoint_, channel));

  return true;
}

void RPC::Destory() {}

bool RPC::CheckMdsAlive(const std::string& addr) {
  EndPoint endpoint;
  butil::str2endpoint(addr.c_str(), &endpoint);

  brpc::ChannelOptions options;
  options.connect_timeout_ms = kConnectTimeoutMs;
  options.timeout_ms = FLAGS_vfs_meta_rpc_timeout_ms;
  options.max_retry = FLAGS_vfs_meta_rpc_retry_times;

  brpc::Channel channel;
  if (channel.Init(butil::ip2str(endpoint.ip).c_str(), endpoint.port,
                   &options) != 0) {
    LOG(ERROR) << fmt::format("[meta.rpc] init channel fail, addr({}).",
                              EndPointToStr(endpoint));

    return false;
  }

  pb::mds::EchoRequest request;
  pb::mds::EchoResponse response;

  request.mutable_info()->set_request_id(std::to_string(utils::TimestampNs()));

  const google::protobuf::MethodDescriptor* method =
      dingofs::pb::mds::MDSService::descriptor()->FindMethodByName("Echo");

  brpc::Controller cntl;
  channel.CallMethod(method, &cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(ERROR) << fmt::format("[meta.rpc] check mds addr fail, addr({}).",
                              addr);
    return false;
  }

  return true;
}

bool RPC::AddEndpoint(const std::string& ip, int port) {
  utils::WriteLockGuard lk(lock_);

  EndPoint endpoint;
  butil::str2endpoint(ip.c_str(), port, &endpoint);
  auto it = channels_.find(endpoint);
  if (it != channels_.end()) {
    return false;
  }

  ChannelSPtr channel = NewChannel(endpoint);
  if (channel == nullptr) {
    return false;
  }

  channels_.insert(std::make_pair(endpoint, channel));

  return true;
}

void RPC::DeleteEndpoint(const std::string& ip, int port) {
  utils::WriteLockGuard lk(lock_);

  EndPoint endpoint;
  butil::str2endpoint(ip.c_str(), port, &endpoint);
  auto it = channels_.find(endpoint);
  if (it != channels_.end()) {
    channels_.erase(it);
  }
}

EndPoint RPC::RandomlyPickupEndPoint() {
  utils::ReadLockGuard lk(lock_);

  std::string source;
  EndPoint endpoint;
  // priority take from active channels
  if (!channels_.empty()) {
    uint32_t random_num =
        mds::Helper::GenerateRealRandomInteger(0, channels_.size());
    uint32_t index = random_num % channels_.size();
    auto it = channels_.begin();
    std::advance(it, index);
    endpoint = it->first;
    source = "active";

  } else if (!fallback_endpoints_.empty()) {
    // take from fallback
    uint32_t random_num =
        mds::Helper::GenerateRealRandomInteger(0, fallback_endpoints_.size());
    uint32_t index = random_num % fallback_endpoints_.size();
    auto it = fallback_endpoints_.begin();
    std::advance(it, index);
    endpoint = *it;
    source = "fallback";

  } else {
    endpoint = init_endpoint_;
    source = "init";
  }

  LOG(INFO) << fmt::format(
      "[meta.rpc] random pickup endpoint, addr({}) source({}).",
      EndPointToStr(endpoint), source);

  return endpoint;
}

void RPC::AddFallbackEndpoint(const EndPoint& endpoint) {
  utils::WriteLockGuard lk(lock_);
  fallback_endpoints_.insert(endpoint);
}

RPC::ChannelSPtr RPC::NewChannel(const EndPoint& endpoint) {  // NOLINT
  CHECK(endpoint.port > 0) << "port is invalid.";

  ChannelSPtr channel = std::make_shared<brpc::Channel>();
  brpc::ChannelOptions options;
  options.connect_timeout_ms = kConnectTimeoutMs;
  options.timeout_ms = FLAGS_vfs_meta_rpc_timeout_ms;
  options.max_retry = FLAGS_vfs_meta_rpc_retry_times;
  if (channel->Init(butil::ip2str(endpoint.ip).c_str(), endpoint.port,
                    &options) != 0) {
    LOG(ERROR) << fmt::format("[meta.rpc] init channel fail, addr({}).",
                              EndPointToStr(endpoint));

    return nullptr;
  }

  return channel;
}

RPC::ChannelSPtr RPC::GetOrAddChannel(const EndPoint& endpoint) {
  {
    utils::ReadLockGuard lk(lock_);

    auto it = channels_.find(endpoint);
    if (it != channels_.end()) {
      return it->second;
    }
  }

  ChannelSPtr channel = NewChannel(endpoint);
  if (channel == nullptr) return nullptr;

  {
    utils::WriteLockGuard lk(lock_);
    channels_[endpoint] = channel;
  }

  return channel;
}

void RPC::DeleteChannel(const EndPoint& endpoint) {
  utils::WriteLockGuard lk(lock_);

  channels_.erase(endpoint);
}

bool RPC::Dump(Json::Value& value) {
  utils::ReadLockGuard lk(lock_);

  value["init_endpoint"] = EndPointToStr(init_endpoint_);

  Json::Value channels = Json::arrayValue;
  for (const auto& pair : channels_) {
    Json::Value item;
    item["endpoint"] = EndPointToStr(pair.first);
    channels.append(item);
  }
  value["channels"] = channels;

  Json::Value fallbacks = Json::arrayValue;
  for (const auto& endpoint : fallback_endpoints_) {
    Json::Value item;
    item["endpoint"] = EndPointToStr(endpoint);
    fallbacks.append(item);
  }

  value["fallbacks"] = fallbacks;

  return true;
}

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs