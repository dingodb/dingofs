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
 * Created Date: 2025-02-08
 * Author: Jingli Chen (Wine93)
 */

#include "mds/cachegroup/cache_group_member_manager.h"

#include <absl/strings/str_format.h>

#include <cstdint>

#include "common/status.h"
#include "mds/cachegroup/cache_group_member_storage.h"
#include "mds/cachegroup/helper.h"

namespace dingofs {
namespace mds {
namespace cachegroup {

CacheGroupMemberManagerImpl::CacheGroupMemberManagerImpl(
    kvstorage::KVStorageClientSPtr storage)
    : running_(false),
      members_(std::make_unique<Members>(storage)),
      groups_(std::make_unique<Groups>(storage)) {}

Status CacheGroupMemberManagerImpl::Start() {
  if (running_) {
    return Status::OK();
  }

  LOG(INFO) << "Starting cache group member manager...";

  auto status = groups_->Load();
  if (!status.ok()) {
    LOG(ERROR) << "Load groups from storage failed: " << status.ToString();
    return status;
  }

  status = members_->Load();
  if (!status.ok()) {
    LOG(ERROR) << "Load members from storage failed: " << status.ToString();
    return status;
  }

  GroupSPtr group;
  auto members = members_->GetAllMembers();
  for (const auto& member : members) {
    auto member_info = member->GetInfo();
    if (!member_info.has_group_name() || member_info.group_name().empty()) {
      continue;
    }

    const auto& group_name = member_info.group_name();
    CHECK_GT(group_name.size(), 0);
    CHECK(groups_->GetGroup(group_name, group).ok());
    group->AddMember(member);
  }

  running_ = true;

  LOG(INFO) << "Cache group member manager is up.";

  return Status::OK();
}

Status CacheGroupMemberManagerImpl::Shutdown() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }

  LOG(INFO) << "Cache group member manager is shutting down...";

  LOG(INFO) << "Cache group member manager is down.";

  return Status::OK();
}

Status CacheGroupMemberManagerImpl::JoinCacheGroup(
    const std::string& group_name, const std::string& ip, uint32_t port,
    uint32_t weight, uint64_t replace_id, uint64_t* member_id,
    std::string* member_uuid) {
  GroupSPtr group;
  auto status = GetOrCreateGroup(group_name, group);
  if (!status.ok()) {
    return status;
  }

  MemberSPtr member;
  if (replace_id != 0) {
    status = members_->ReplaceMember(replace_id, ip, port, member);
    if (!status.ok()) {
      LOG(ERROR) << "Replace member failed: replace_id = " << replace_id
                 << ", ip = " << ip << ", port = " << port
                 << ", status = " << status.ToString();
      return status;
    }
  } else {
    status = GetOrCreateMember(ip, port, member);
  }
  if (!status.ok()) {
    return status;
  }

  status = member->JoinCacheGroup(group, weight);
  if (status.ok()) {
    *member_id = member->GetInfo().id();
    *member_uuid = member->GetInfo().uuid();
  }

  if (status.ok()) {
    LOG(INFO) << "Joined cache group success: member = "
              << member->GetInfo().ShortDebugString();
  } else {
    LOG(ERROR) << "Join cache group failed: group_name = " << group_name
               << ", replace_id = " << replace_id << ", ip = " << ip
               << ", port = " << port << ", weight = " << weight
               << ", status = " << status.ToString();
  }
  return Status::OK();
}

Status CacheGroupMemberManagerImpl::LeaveCacheGroup(
    const std::string& group_name, const std::string& ip, uint32_t port) {
  GroupSPtr group;
  auto status = groups_->GetGroup(group_name, group);
  if (!status.ok()) {
    LOG(ERROR) << "Get group failed: group_name = " << group_name
               << ", status = " << status.ToString();
    return status;
  }

  MemberSPtr member;
  status = members_->GetMember(ip, port, member);
  if (!status.ok()) {
    LOG(ERROR) << "Get member failed: ip = " << ip << ", port = " << port
               << ", status = " << status.ToString();
    return status;
  }

  status = member->LeaveCacheGroup();
  if (!status.ok()) {
    LOG(ERROR) << "Leave cache group failed: group_name = " << group_name
               << ", ip = " << ip << ", port = " << port
               << ", status = " << status.ToString();
    return status;
  }
  return Status::OK();
}

Status CacheGroupMemberManagerImpl::Heartbeat(const std::string& ip,
                                              uint32_t port) {
  MemberSPtr member;
  auto status = members_->GetMember(ip, port, member);
  if (!status.ok()) {
    LOG(ERROR) << "Get member failed: ip = " << ip << ", port = " << port
               << ", status = " << status.ToString();
    return status;
  }
  return member->Heartbeat(Helper::TimestampMs());
}

std::vector<std::string> CacheGroupMemberManagerImpl::GetGroupNames() {
  return groups_->GetAllGroupNames();
}

Status CacheGroupMemberManagerImpl::GetMembers(
    const std::string& group_name,
    std::vector<PBCacheGroupMember>* members_out) {
  GroupSPtr group;
  auto status = groups_->GetGroup(group_name, group);
  if (!status.ok()) {
    LOG(ERROR) << "Get group failed: group_name = " << group_name
               << ", status = " << status.ToString();
    return status;
  }

  std::vector<MemberSPtr> members = group->GetAllMembers();
  for (const auto& member : members) {
    members_out->emplace_back(member->GetInfo());
  }
  return Status::OK();
}

Status CacheGroupMemberManagerImpl::ReweightMember(uint64_t member_id,
                                                   uint32_t weight) {
  MemberSPtr member;
  auto status = members_->GetMember(member_id, member);
  if (!status.ok()) {
    LOG(ERROR) << "Get member failed: member_id = " << member_id
               << ", status = " << status.ToString();
    return status;
  }

  status = member->Reweight(weight);
  if (!status.ok()) {
    LOG(ERROR) << "Reweight member failed: member_id = " << member_id
               << ", weight = " << weight << ", status = " << status.ToString();
  }
  return status;
}

Status CacheGroupMemberManagerImpl::GetOrCreateGroup(
    const std::string& group_name, GroupSPtr& group) {
  auto status = groups_->GetGroup(group_name, group);
  if (status.ok()) {
    return Status::OK();
  } else if (!status.IsNotFound()) {
    LOG(ERROR) << "Get group failed: group_name = " << group_name
               << ", status = " << status.ToString();
    return status;
  }

  status = groups_->CreateGroup(group_name, group);
  if (status.ok() || status.IsExist()) {
    status = Status::OK();
  }

  if (status.ok()) {
    LOG(INFO) << "Create group success: group_name = " << group_name;
  } else {
    LOG(ERROR) << "Create group failed: group_name = " << group_name
               << ", status = " << status.ToString();
  }
  return status;
}

Status CacheGroupMemberManagerImpl::GetOrCreateMember(const std::string& ip,
                                                      uint32_t port,
                                                      MemberSPtr& member) {
  auto status = members_->GetMember(ip, port, member);
  if (status.ok()) {
    return status;
  } else if (!status.IsNotFound()) {
    LOG(ERROR) << "Get member failed: ip = " << ip << ", port = " << port
               << ", status = " << status.ToString();
    return status;
  }

  status = members_->CreateMember(ip, port, member);
  if (status.ok() || status.IsExist()) {
    status = Status::OK();
  }

  if (status.ok()) {
    LOG(INFO) << "Create member success: member = "
              << member->GetInfo().DebugString();
  } else {
    LOG(ERROR) << "Create member failed: ip = " << ip << ", port = " << port
               << ", status = " << status.ToString();
  }
  return status;
}

}  // namespace cachegroup
}  // namespace mds
}  // namespace dingofs
