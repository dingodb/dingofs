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

#include "dingofs/src/client/filesystemv2/rpc.h"

#include "bthread/mutex.h"

namespace dingofs {
namespace client {
namespace filesystem {

DEFINE_int32(rpc_retry_times, 3, "rpc retry time");
BRPC_VALIDATE_GFLAG(rpc_retry_times, brpc::PositiveInteger);

RPC::RPC() { bthread_mutex_init(&mutex_, nullptr); }

RPC::~RPC() { bthread_mutex_destroy(&mutex_); }

RPC::Channel* RPC::GetChannel(EndPoint endpoint) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = channels_.find(endpoint);
  if (it != channels_.end()) {
    return it->second.get();
  }

  ChannelPtr channel(new Channel());
  if (channel->Init(endpoint.GetIp().c_str(), endpoint.GetPort(), nullptr) !=
      0) {
    LOG(ERROR) << "Fail to init channel to " << endpoint.GetIp() << ":"
               << endpoint.GetPort();

    return nullptr;
  }

  channels_[endpoint] = std::move(channel);
  return channels_[endpoint].get();
}

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs