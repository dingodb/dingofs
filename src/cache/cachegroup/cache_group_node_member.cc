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
 * Created Date: 2025-03-17
 * Author: Jingli Chen (Wine93)
 */

#include "cache/cachegroup/cache_group_node_member.h"

#include "common/options/cache.h"

namespace dingofs {
namespace cache {

CacheGroupNodeMemberImpl::CacheGroupNodeMemberImpl(MDSClientSPtr mds_client)
    : mds_client_(mds_client) {}

Status CacheGroupNodeMemberImpl::JoinGroup() {
  CHECK_NOTNULL(mds_client_);

  auto status = mds_client_->JoinCacheGroup(FLAGS_id, FLAGS_listen_ip,
                                            FLAGS_listen_port, FLAGS_group_name,
                                            FLAGS_group_weight, &member_id_);
  if (!status.ok()) {
    LOG(ERROR) << "Join cache group failed: group_name = " << FLAGS_group_name
               << ", ip = " << FLAGS_listen_ip
               << ", port = " << FLAGS_listen_port
               << ", weight = " << FLAGS_group_weight
               << ", status = " << status.ToString();
    return Status::Internal("join cache group failed");
  }

  LOG(INFO) << "Join cache group success: group_name = " << FLAGS_group_name
            << ", ip = " << FLAGS_listen_ip << ", port = " << FLAGS_listen_port
            << ", weight = " << FLAGS_group_weight
            << ", member_id = " << member_id_;

  return Status::OK();
}

Status CacheGroupNodeMemberImpl::LeaveGroup() {
  CHECK_NOTNULL(mds_client_);

  auto status = mds_client_->LeaveCacheGroup(
      FLAGS_id, FLAGS_listen_ip, FLAGS_listen_port, FLAGS_group_name);
  if (!status.ok()) {
    LOG(ERROR) << "Leave cache group failed: group_name = " << FLAGS_group_name
               << ", ip = " << FLAGS_listen_ip
               << ", port = " << FLAGS_listen_port
               << ", status = " << status.ToString();
    return Status::Internal("leave cache group failed");
  }

  LOG(INFO) << "Leave cache group success: group_name = " << FLAGS_group_name
            << ", ip = " << FLAGS_listen_ip << ", port = " << FLAGS_listen_port;
  return Status::OK();
}

std::string CacheGroupNodeMemberImpl::GetGroupName() const {
  return FLAGS_group_name;
}

std::string CacheGroupNodeMemberImpl::GetListenIP() const {
  return FLAGS_listen_ip;
}

uint32_t CacheGroupNodeMemberImpl::GetListenPort() const {
  return FLAGS_listen_port;
}

std::string CacheGroupNodeMemberImpl::GetMemberId() const { return member_id_; }

}  // namespace cache
}  // namespace dingofs
