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

#include "cache/common/proto.h"

namespace dingofs {
namespace cache {

DEFINE_uint64(replace_id, 0,
              "The ID to replace when joining the cache group. "
              "If set to 0, no replacement will be done.");

CacheGroupNodeMemberImpl::CacheGroupNodeMemberImpl(CacheGroupNodeOption option,
                                                   MdsClientSPtr mds_client)
    : member_id_(0),
      member_uuid_(""),
      option_(option),
      mds_client_(mds_client) {}

Status CacheGroupNodeMemberImpl::JoinGroup() {
  CHECK_NOTNULL(mds_client_);

  auto rc = mds_client_->JoinCacheGroup(
      option_.group_name, option_.listen_ip, option_.listen_port,
      option_.group_weight, option_.replace_id, &member_id_, &member_uuid_);
  if (rc != PBCacheGroupErrCode::CacheGroupOk) {
    LOG(ERROR) << "Join cache group failed: group_name = " << option_.group_name
               << ", ip = " << option_.listen_ip
               << ", port = " << option_.listen_port
               << ", weight = " << option_.group_weight
               << ", replace_id = " << option_.replace_id
               << ", rc = " << CacheGroupErrCode_Name(rc);
    return Status::Internal("join cache group failed");
  }

  LOG(INFO) << "Join cache group success: group_name = " << option_.group_name
            << ", ip = " << option_.listen_ip
            << ", port = " << option_.listen_port
            << ", weight = " << option_.group_weight
            << ", replace_id = " << option_.replace_id
            << ", member_id = " << member_id_
            << ", member_uuid = " << member_uuid_;

  return Status::OK();
}

Status CacheGroupNodeMemberImpl::LeaveGroup() {
  CHECK_NOTNULL(mds_client_);

  auto rc = mds_client_->LeaveCacheGroup(option_.group_name, option_.listen_ip,
                                         option_.listen_port);
  if (rc != PBCacheGroupErrCode::CacheGroupOk) {
    LOG(ERROR) << "Leave cache group failed: group_name = "
               << option_.group_name << ", ip = " << option_.listen_ip
               << ", port = " << option_.listen_port
               << ", rc = " << CacheGroupErrCode_Name(rc);
    return Status::Internal("leave cache group failed");
  }

  LOG(INFO) << "Leave cache group success: group_name = " << option_.group_name
            << ", ip = " << option_.listen_ip
            << ", port = " << option_.listen_port;
  return Status::OK();
}

std::string CacheGroupNodeMemberImpl::GetGroupName() const {
  return option_.group_name;
}

std::string CacheGroupNodeMemberImpl::GetListenIP() const {
  return option_.listen_ip;
}

uint32_t CacheGroupNodeMemberImpl::GetListenPort() const {
  return option_.listen_port;
}

uint64_t CacheGroupNodeMemberImpl::GetMemberId() const { return member_id_; }

std::string CacheGroupNodeMemberImpl::GetMemberUuid() const {
  return member_uuid_;
}

}  // namespace cache
}  // namespace dingofs
