/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Project: DingoFS
 * Created Date: 2025-01-13
 * Author: Jingli Chen (Wine93)
 */

#include "cache/cachegroup/cache_group_node_heartbeat.h"

#include "cache/common/macro.h"
#include "utils/executor/bthread/bthread_executor.h"

namespace dingofs {
namespace cache {

DEFINE_uint32(send_heartbeat_interval_s, 3,
              "Interval to send heartbeat to MDS in seconds");

CacheGroupNodeHeartbeatImpl::CacheGroupNodeHeartbeatImpl(
    CacheGroupNodeMemberSPtr member, MDSClientSPtr mds_client)
    : running_(false),
      member_(member),
      mds_client_(mds_client),
      executor_(std::make_unique<BthreadExecutor>()) {}

void CacheGroupNodeHeartbeatImpl::Start() {
  CHECK_NOTNULL(member_);
  CHECK_NOTNULL(mds_client_);
  CHECK_NOTNULL(executor_);

  if (running_) {
    return;
  }

  LOG(INFO) << "Cache group node heartbeat is starting...";

  CHECK(executor_->Start());
  executor_->Schedule([this] { SendHeartbeat(); },
                      FLAGS_send_heartbeat_interval_s * 1000);

  running_ = true;

  LOG(INFO) << "Cache group node heartbeat is up.";

  CHECK_RUNNING("Cache group node heartbeat");
}

void CacheGroupNodeHeartbeatImpl::Shutdown() {
  if (!running_.exchange(false)) {
    return;
  }

  LOG(INFO) << "Cache group node heartbeat is shutting down...";

  CHECK(executor_->Stop());

  LOG(INFO) << "Cache group node heartbeat is down.";

  CHECK_DOWN("Cache group node heartbeat");
}

void CacheGroupNodeHeartbeatImpl::SendHeartbeat() {
  std::string group_name = member_->GetGroupName();
  auto status = mds_client_->Heartbeat(
      member_->GetMemberId(), member_->GetListenIP(), member_->GetListenPort());
  if (!status.ok()) {
    LOG(ERROR) << "Send cache group member heartbeat failed: group_name = "
               << group_name << ", ip = " << member_->GetListenIP()
               << ", port = " << member_->GetListenPort()
               << ", status = " << status.ToString();
  }
  executor_->Schedule([this] { SendHeartbeat(); },
                      FLAGS_send_heartbeat_interval_s * 1000);
}

}  // namespace cache
}  // namespace dingofs
