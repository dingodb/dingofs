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

#include "mds/service/service_access.h"

#include "butil/endpoint.h"
#include "common/logging.h"
#include "dingofs/error.pb.h"
#include "dingofs/mds.pb.h"
#include "fmt/core.h"
#include "mds/common/helper.h"

namespace dingofs {
namespace mds {

static const uint32_t kConnectTimeoutMs = 200;
static const uint32_t kRpcTimeoutMs = 6000;

ChannelPool::ChannelPool() { bthread_mutex_init(&mutex_, nullptr); }
ChannelPool::~ChannelPool() { bthread_mutex_destroy(&mutex_); }

ChannelPool& ChannelPool::GetInstance() {
  static ChannelPool instance;
  return instance;
}

std::shared_ptr<brpc::Channel> ChannelPool::GetChannel(const butil::EndPoint& endpoint) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = channels_.find(endpoint);
  if (it != channels_.end()) {
    return it->second;
  }

  // Create new channel
  auto channel = std::make_shared<brpc::Channel>();
  brpc::ChannelOptions options;
  options.connect_timeout_ms = kConnectTimeoutMs;
  options.timeout_ms = kRpcTimeoutMs;
  options.backup_request_ms = kRpcTimeoutMs;
  options.connection_type = brpc::ConnectionType::CONNECTION_TYPE_SINGLE;
  if (channel->Init(endpoint, nullptr) != 0) {
    LOG(ERROR) << "init channel fail, endpoint: " << Helper::EndPointToString(endpoint);
    return nullptr;
  }

  channels_.insert(std::make_pair(endpoint, channel));
  return channel;
}

Status ServiceAccess::CheckAlive(const butil::EndPoint& endpoint) {
  auto channel = ChannelPool::GetInstance().GetChannel(endpoint);
  if (channel == nullptr) {
    return Status(pb::error::EINTERNAL, "get channel fail");
  }

  pb::mds::MDSService_Stub stub(channel.get());

  brpc::Controller cntl;
  cntl.set_timeout_ms(kRpcTimeoutMs);

  pb::mds::CheckAliveRequest request;
  pb::mds::CheckAliveResponse response;

  stub.CheckAlive(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG_DEBUG << "send request fail, " << cntl.ErrorText();
    return Status(pb::error::EINTERNAL, cntl.ErrorText());
  }

  return Status::OK();
}

Status ServiceAccess::NotifyBuddy(const butil::EndPoint& endpoint, const pb::mds::NotifyBuddyRequest& request) {
  auto channel = ChannelPool::GetInstance().GetChannel(endpoint);
  if (channel == nullptr) {
    return Status(pb::error::EINTERNAL, "get channel fail");
  }

  pb::mds::MDSService_Stub stub(channel.get());

  brpc::Controller cntl;
  cntl.set_timeout_ms(kRpcTimeoutMs);

  pb::mds::NotifyBuddyResponse response;

  stub.NotifyBuddy(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(ERROR) << "send request fail, " << cntl.ErrorText();
    return Status(pb::error::EINTERNAL, cntl.ErrorText());
  }

  return Status::OK();
}

}  // namespace mds
}  // namespace dingofs