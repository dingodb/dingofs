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

#include "mdsv2/service/service_access.h"

#include "butil/endpoint.h"
#include "dingofs/error.pb.h"
#include "dingofs/mdsv2.pb.h"
#include "fmt/core.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"

namespace dingofs {
namespace mdsv2 {

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
  options.connect_timeout_ms = 4000;
  options.timeout_ms = 6000;
  options.backup_request_ms = 5000;
  options.connection_type = brpc::ConnectionType::CONNECTION_TYPE_SINGLE;
  if (channel->Init(endpoint, nullptr) != 0) {
    DINGO_LOG(ERROR) << "init channel fail, endpoint: " << Helper::EndPointToString(endpoint);
    return nullptr;
  }

  channels_.insert(std::make_pair(endpoint, channel));
  return channel;
}

Status ServiceAccess::RefreshFsInfo(const butil::EndPoint& endpoint, const std::string& fs_name) {
  auto channel = ChannelPool::GetInstance().GetChannel(endpoint);
  if (channel == nullptr) {
    return Status(pb::error::EINTERNAL, "get channel fail");
  }

  pb::mdsv2::MDSService_Stub stub(channel.get());

  brpc::Controller cntl;
  cntl.set_timeout_ms(6000);

  pb::mdsv2::RefreshFsInfoRequest request;
  request.set_fs_name(fs_name);
  pb::mdsv2::RefreshFsInfoResponse response;

  stub.RefreshFsInfo(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(ERROR) << "send request fail, " << cntl.ErrorText();
    return Status(pb::error::EINTERNAL, cntl.ErrorText());
  }

  if (response.error().errcode() != pb::error::OK) {
    DINGO_LOG(ERROR) << fmt::format("refresh fs info fail, error: {} {}",
                                    pb::error::Errno_Name(response.error().errcode()), response.error().errmsg());

    return Status(response.error().errcode(), response.error().errmsg());
  }

  return Status::OK();
}

}  // namespace mdsv2
}  // namespace dingofs