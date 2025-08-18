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

#include "cache/common/rpc.h"

#include <brpc/channel.h>
#include <brpc/reloadable_flags.h>
#include <butil/endpoint.h>
#include <fmt/format.h>
#include <glog/logging.h>

#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace cache {

DEFINE_uint32(mdsv2_rpc_timeout_ms, 60000, "mdsv2 rpc timeout ms");
DEFINE_validator(mdsv2_rpc_timeout_ms, brpc::PassValidate);

DEFINE_uint32(mdsv2_rpc_retry_times, 3, "mdsv2 rpc retry time");
DEFINE_validator(mdsv2_rpc_retry_times, brpc::PassValidate);

const int32_t kConnectTimeoutMs = 200;  // milliseconds

RPC::RPC(const std::string& addr) {
  butil::EndPoint endpoint;
  butil::str2endpoint(addr.c_str(), &endpoint);
  default_endpoint_ = endpoint;
}

RPC::RPC(const std::string& ip, int port) {
  butil::EndPoint endpoint;
  butil::str2endpoint(ip.c_str(), port, &endpoint);
  default_endpoint_ = endpoint;
}

RPC::RPC(const butil::EndPoint& endpoint) : default_endpoint_(endpoint) {}

bool RPC::Init() {
  ChannelPtr channel = NewChannel(default_endpoint_);
  if (channel == nullptr) {
    return false;
  }
  channels_.insert(std::make_pair(default_endpoint_, channel));

  return true;
}

void RPC::Destory() {}

bool RPC::AddEndpoint(const std::string& ip, int port, bool is_default) {
  utils::WriteLockGuard lk(lock_);

  butil::EndPoint endpoint;
  butil::str2endpoint(ip.c_str(), port, &endpoint);
  auto it = channels_.find(endpoint);
  if (it != channels_.end()) {
    return false;
  }

  ChannelPtr channel = NewChannel(endpoint);
  if (channel == nullptr) {
    return false;
  }

  if (is_default) {
    default_endpoint_ = endpoint;
  }

  channels_.insert(std::make_pair(endpoint, channel));

  return true;
}

void RPC::DeleteEndpoint(const std::string& ip, int port) {
  utils::WriteLockGuard lk(lock_);

  butil::EndPoint endpoint;
  butil::str2endpoint(ip.c_str(), port, &endpoint);
  auto it = channels_.find(endpoint);
  if (it != channels_.end()) {
    channels_.erase(it);
  }
}

RPC::ChannelPtr RPC::NewChannel(const butil::EndPoint& endpoint) {  // NOLINT
  CHECK(endpoint.port > 0) << "port is invalid.";

  ChannelPtr channel = std::make_shared<brpc::Channel>();
  brpc::ChannelOptions options;
  options.connect_timeout_ms = kConnectTimeoutMs;
  options.timeout_ms = FLAGS_mdsv2_rpc_timeout_ms;
  options.max_retry = FLAGS_mdsv2_rpc_retry_times;
  if (channel->Init(butil::ip2str(endpoint.ip).c_str(), endpoint.port,
                    &options) != 0) {
    LOG(ERROR) << fmt::format("[meta.rpc] init channel fail, addr({}).",
                              EndPointToStr(endpoint));

    return nullptr;
  }

  return channel;
}

RPC::Channel* RPC::GetChannel(const butil::EndPoint& endpoint) {
  utils::ReadLockGuard lk(lock_);

  auto it = channels_.find(endpoint);
  if (it != channels_.end()) {
    return it->second.get();
  }

  ChannelPtr channel = NewChannel(endpoint);
  if (channel == nullptr) {
    return nullptr;
  }

  channels_[endpoint] = std::move(channel);
  return channels_[endpoint].get();
}

void RPC::DeleteChannel(const butil::EndPoint& endpoint) {
  utils::WriteLockGuard lk(lock_);

  channels_.erase(endpoint);
}

}  // namespace cache
}  // namespace dingofs
