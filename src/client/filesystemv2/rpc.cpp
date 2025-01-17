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

#include "client/filesystemv2/rpc.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include "bthread/mutex.h"

namespace dingofs {
namespace client {
namespace filesystem {

DEFINE_int32(rpc_retry_times, 3, "rpc retry time");
BRPC_VALIDATE_GFLAG(rpc_retry_times, brpc::PositiveInteger);

RPC::RPC() {
  CHECK(bthread_mutex_init(&mutex_, nullptr) == 0) << "init mutex fail.";
}

RPC::~RPC() {
  CHECK(bthread_mutex_destroy(&mutex_) == 0) << "destroy mutex fail.";
}

bool RPC::Init() { return true; }

void RPC::Destory() {}

bool RPC::AddEndpoint(const std::string& ip, int port, bool is_default) {
  BAIDU_SCOPED_LOCK(mutex_);

  EndPoint endpoint(ip, port);
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
  BAIDU_SCOPED_LOCK(mutex_);

  EndPoint endpoint(ip, port);
  auto it = channels_.find(endpoint);
  if (it != channels_.end()) {
    channels_.erase(it);
  }
}

RPC::ChannelPtr RPC::NewChannel(const EndPoint& endpoint) {
  CHECK(!endpoint.GetIp().empty()) << "ip is empty.";
  CHECK(endpoint.GetPort() > 0) << "port is invalid.";

  ChannelPtr channel = std::make_shared<brpc::Channel>();
  if (channel->Init(endpoint.GetIp().c_str(), endpoint.GetPort(), nullptr) !=
      0) {
    LOG(ERROR) << fmt::format("init channel fail, addr({}:{})",
                              endpoint.GetIp(), endpoint.GetPort());

    return nullptr;
  }

  return channel;
}

RPC::Channel* RPC::GetChannel(EndPoint endpoint) {
  BAIDU_SCOPED_LOCK(mutex_);

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

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs