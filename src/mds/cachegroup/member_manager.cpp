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

#include "mds/cachegroup/member_manager.h"

#include <absl/strings/str_format.h>
#include <butil/time.h>
#include <glog/logging.h>

#include <cstdint>
#include <string>

#include "common/status.h"
#include "mds/cachegroup/common.h"
#include "mds/cachegroup/group.h"
#include "mds/cachegroup/helper.h"
#include "mds/cachegroup/member.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace mds {
namespace cachegroup {

using utils::ReadLockGuard;
using utils::WriteLockGuard;

CacheGroupMemberManagerImpl::CacheGroupMemberManagerImpl(
    kvstorage::KVStorageClientSPtr storage)
    : running_(false),
      members_(std::make_unique<Members>(storage)),
      groups_(std::make_unique<Groups>(storage)) {}

Status CacheGroupMemberManagerImpl::Start() {
  CHECK_NOTNULL(members_);
  CHECK_NOTNULL(groups_);

  if (running_) {
    return Status::OK();
  }

  LOG(INFO) << "Cache group member manager is starting...";

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
    auto member_info = member->Info();
    if (!member_info.has_group_name()) {
      continue;
    }

    const auto& group_name = member_info.group_name();
    CHECK_GT(group_name.length(), 0);

    CHECK(groups_->GetGroup(group_name, group).ok());
    group->AddMember(member_info.id(), member);
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

  // do nothing

  LOG(INFO) << "Cache group member manager is down.";

  return Status::OK();
}

Status CacheGroupMemberManagerImpl::RegisterMember(const EndPoint& endpoint,
                                                   const std::string& want_id,
                                                   std::string* member_id) {
  WriteLockGuard lock(rwlock_);
  MemberSPtr member;
  auto status = members_->RegisterMember(endpoint, want_id, member);
  if (!status.ok()) {
    LOG(ERROR) << "Register member failed: endpoint = " << endpoint.ToString()
               << ", want_id = " << want_id
               << ", status = " << status.ToString();
    return status;
  }

  auto info = member->Info();
  *member_id = info.id();
  CHECK_EQ(*member_id, want_id);

  LOG(INFO) << "Register member success: endpoint = " << endpoint.ToString()
            << ", want_id = " << want_id
            << ", member = " << info.ShortDebugString();
  return Status::OK();
}

Status CacheGroupMemberManagerImpl::DeregisterMember(const EndPoint& endpoint) {
  WriteLockGuard lock(rwlock_);
  PBCacheGroupMember old_info;
  auto status = members_->DeregisterMember(endpoint, &old_info);
  if (!status.ok()) {
    LOG(ERROR) << "Deregister member failed: endpoint = " << endpoint.ToString()
               << ", status = " << status.ToString();
    return status;
  }

  auto member_id = old_info.id();
  auto group_name = old_info.group_name();
  LeaveGroup(group_name, member_id);

  LOG(INFO) << "Deregister member success: endpoint = " << endpoint.ToString();
  return Status::OK();
}

Status CacheGroupMemberManagerImpl::DeleteMemberId(
    const std::string& member_id) {
  WriteLockGuard lock(rwlock_);
  auto status = members_->DeleteMemberId(member_id);
  if (!status.ok()) {
    LOG(ERROR) << "Delete member id failed: member_id = " << member_id
               << ", status = " << status.ToString();
    return status;
  }

  LOG(INFO) << "Delete member id success: member_id = " << member_id;
  return Status::OK();
}

Status CacheGroupMemberManagerImpl::MemberHeartbeat(const EndPoint& endpoint) {
  MemberSPtr member;
  {
    ReadLockGuard lock(rwlock_);
    auto status = members_->GetMember(endpoint, member);
    if (!status.ok()) {
      LOG(ERROR) << "Get member failed: endpoint = " << endpoint.ToString()
                 << ", status = " << status.ToString();
      return status;
    }
  }

  auto status = member->SetLastOnlineTime(Helper::TimestampMs());
  if (!status.ok()) {
    LOG(ERROR) << "Member heartbeat failed: endpoint = " << endpoint.ToString()
               << ", member = " << member->Info().ShortDebugString()
               << ", status = " << status.ToString();
  }
  return status;
}

Status CacheGroupMemberManagerImpl::ReweightMember(const std::string& member_id,
                                                   uint32_t weight) {
  MemberSPtr member;
  {
    ReadLockGuard lock(rwlock_);
    auto status = members_->GetMember(member_id, member);
    if (!status.ok()) {
      LOG(ERROR) << "Get member failed: member_id = " << member_id
                 << ", status = " << status.ToString();
      return status;
    }
  }

  auto status = member->SetWeight(weight);
  if (!status.ok()) {
    LOG(ERROR) << "Reweight member failed: member_id = " << member_id
               << ", weight = " << weight << ", status = " << status.ToString();
    return status;
  }

  LOG(INFO) << "Reweight member success: member_id = " << member_id
            << ", weight = " << weight
            << ", member = " << member->Info().ShortDebugString();
  return Status::OK();
}

std::vector<PBCacheGroupMember> CacheGroupMemberManagerImpl::ListAllMembers() {
  ReadLockGuard lock(rwlock_);
  return members_->GetAllInfos();
}

Status CacheGroupMemberManagerImpl::JoinCacheGroup(
    const std::string& group_name, const EndPoint& endpoint, uint32_t weight,
    std::string* member_id) {
  butil::Timer timer;
  timer.start();

  WriteLockGuard lock(rwlock_);
  GroupSPtr group;
  auto status = GetOrRegisterGroup(group_name, group);
  if (!status.ok()) {
    return status;
  }

  MemberSPtr member;
  status = GetOrRegisterMember(endpoint, member);
  if (!status.ok()) {
    return status;
  }

  auto info = member->Info();
  const auto& old_group_name = info.group_name();
  auto old_weight = info.weight();
  if (old_group_name == group_name && old_weight == weight) {
    LOG(INFO) << "Member already in the group with same weight: group_name = "
              << group_name << ", endpoint = " << endpoint.ToString()
              << ", weight = " << weight
              << ", member = " << info.ShortDebugString();
    *member_id = info.id();
    return Status::OK();
  }

  status = member->SetGroup(group_name, weight);
  if (!status.ok()) {
    LOG(ERROR) << "Member join cache group failed: group_name = " << group_name
               << ", endpoint = " << endpoint.ToString()
               << ", weight = " << weight << ", status = " << status.ToString();
    return status;
  }

  LeaveGroup(old_group_name, info.id());
  group->AddMember(info.id(), member);
  *member_id = info.id();

  timer.stop();

  LOG(INFO) << "Member join cache group success: group_name = " << group_name
            << ", member = " << member->Info().ShortDebugString() << ", cost "
            << timer.u_elapsed() / 1e6 << " seconds.";
  return Status::OK();
}

Status CacheGroupMemberManagerImpl::LeaveCacheGroup(
    const std::string& group_name, const EndPoint& endpoint) {
  WriteLockGuard lock(rwlock_);
  MemberSPtr member;
  auto status = members_->GetMember(endpoint, member);
  if (!status.ok()) {
    LOG(ERROR) << "Get member failed: endpoint = " << endpoint.ToString()
               << ", status = " << status.ToString();
    return status;
  }
  return LeaveGroup(group_name, member);
}

Status CacheGroupMemberManagerImpl::LeaveCacheGroup(
    const std::string& group_name, const std::string& member_id) {
  WriteLockGuard lock(rwlock_);
  MemberSPtr member;
  auto status = members_->GetMember(member_id, member);
  if (!status.ok()) {
    LOG(ERROR) << "Get member failed: member_id = " << member_id
               << ", status = " << status.ToString();
    return status;
  }
  return LeaveGroup(group_name, member);
}

std::vector<std::string> CacheGroupMemberManagerImpl::ListGroups() {
  ReadLockGuard lock(rwlock_);
  return groups_->GetAllGroupNames();
}

Status CacheGroupMemberManagerImpl::ListGroupMembers(
    const std::string& group_name, std::vector<PBCacheGroupMember>* members) {
  ReadLockGuard lock(rwlock_);
  GroupSPtr group;
  auto status = groups_->GetGroup(group_name, group);
  if (!status.ok()) {
    LOG(ERROR) << "Get group failed: group_name = " << group_name
               << ", status = " << status.ToString();
    return status;
  }

  for (const auto& member : group->GetAllMembers()) {
    members->emplace_back(member->Info());
  }
  return Status::OK();
}

Status CacheGroupMemberManagerImpl::GetOrRegisterGroup(
    const std::string& group_name, GroupSPtr& group) {
  auto status = groups_->GetGroup(group_name, group);
  if (status.ok()) {
    return Status::OK();
  } else if (!status.IsNotFound()) {
    LOG(ERROR) << "Get group failed: group_name = " << group_name
               << ", status = " << status.ToString();
    return status;
  }

  status = groups_->RegisterGroup(group_name, group);
  if (status.ok() || status.IsExist()) {
    status = Status::OK();
  }

  if (status.ok()) {
    LOG(INFO) << "Register group success: group_name = " << group_name;
  } else {
    LOG(ERROR) << "Register group failed: group_name = " << group_name
               << ", status = " << status.ToString();
  }
  return status;
}

Status CacheGroupMemberManagerImpl::GetOrRegisterMember(
    const EndPoint& endpoint, MemberSPtr& member) {
  auto status = members_->GetMember(endpoint, member);
  if (status.ok()) {
    return status;
  } else if (!status.IsNotFound()) {
    LOG(ERROR) << "Get member failed: endpoint = " << endpoint.ToString()
               << ", status = " << status.ToString();
    return status;
  }

  status = members_->RegisterMember(endpoint, member);
  if (status.ok() || status.IsExist()) {
    status = Status::OK();
  }

  if (status.ok()) {
    LOG(INFO) << "Register member success: member = "
              << member->Info().ShortDebugString();
  } else {
    LOG(ERROR) << "Register member failed: endpoint = " << endpoint.ToString()
               << ", status = " << status.ToString();
  }
  return status;
}

Status CacheGroupMemberManagerImpl::LeaveGroup(const std::string& group_name,
                                               MemberSPtr member) {
  butil::Timer timer;
  timer.start();

  GroupSPtr group;
  auto status = groups_->GetGroup(group_name, group);
  if (!status.ok()) {
    LOG(ERROR) << "Get group failed: group_name = " << group_name
               << ", status = " << status.ToString();
    return status;
  }

  auto info = member->Info();
  if (info.group_name() != group_name) {
    LOG(ERROR) << "Member not in the group: group_name = " << group_name
               << ", member = " << info.ShortDebugString();
    return Status::InvalidParam("member not in the group");
  }

  status = member->ClearGroup();
  if (!status.ok()) {
    LOG(ERROR) << "Clear member group failed: group_name = " << group_name
               << ", member = " << info.ShortDebugString()
               << ", status = " << status.ToString();
    return status;
  }

  LeaveGroup(group_name, member->Info().id());

  timer.stop();

  LOG(INFO) << "Member leave cache group success: group_name = " << group_name
            << ", member = " << member->Info().ShortDebugString() << ", cost "
            << timer.u_elapsed() / 1e6 << " seconds.";

  return Status::OK();
}

void CacheGroupMemberManagerImpl::LeaveGroup(const std::string& group_name,
                                             const std::string& member_id) {
  if (group_name.empty()) {
    return;
  }

  GroupSPtr group;
  CHECK(groups_->GetGroup(group_name, group).ok());
  group->RemoveMember(member_id);

  LOG(INFO) << "Leave group success: group_name = " << group_name
            << ", member_id = " << member_id;
}

}  // namespace cachegroup
}  // namespace mds
}  // namespace dingofs
