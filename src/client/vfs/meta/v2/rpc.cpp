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

#include "client/vfs/meta/v2/rpc.h"

#include <brpc/channel.h>
#include <butil/endpoint.h>
#include <fmt/format.h>
#include <glog/logging.h>

#include "utils/concurrent/concurrent.h"

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

  uint32_t random_num =
      mdsv2::Helper::GenerateRealRandomInteger(0, channels_.size());
  if (!channels_.empty()) {
    // priority take from active channels
    uint32_t index = random_num % channels_.size();
    auto it = channels_.begin();
    std::advance(it, index);
    return it->first;

  } else if (!fallback_endpoints_.empty()) {
    // take from fallback
    uint32_t index = random_num % fallback_endpoints_.size();
    auto it = fallback_endpoints_.begin();
    std::advance(it, index);
    return *it;
  }

  return init_endpoint_;
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
  options.timeout_ms = FLAGS_client_vfs_rpc_timeout_ms;
  options.max_retry = FLAGS_client_vfs_rpc_retry_times;
  if (channel->Init(butil::ip2str(endpoint.ip).c_str(), endpoint.port,
                    &options) != 0) {
    LOG(ERROR) << fmt::format("[meta.rpc] init channel fail, addr({}).",
                              EndPointToStr(endpoint));

    return nullptr;
  }

  return channel;
}

RPC::ChannelSPtr RPC::GetChannel(const EndPoint& endpoint) {
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

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs