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
 * Created Date: 2025-08-09
 * Author: Jingli Chen (Wine93)
 */

#include "mds/cachegroup/group.h"

#include <glog/logging.h>

#include <memory>
#include <string>

#include "common/status.h"
#include "mds/cachegroup/codec.h"
#include "mds/cachegroup/helper.h"
#include "mds/common/storage_key.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace mds {
namespace cachegroup {

using utils::ReadLockGuard;
using utils::WriteLockGuard;

// group
Group::Group(const std::string& group_name) : group_name_(group_name) {}

void Group::AddMember(const std::string& member_id, MemberSPtr member) {
  id2member_[member_id] = member;
  LOG(INFO) << "Add member to group success: group_name = " << group_name_
            << ", member = " << member->Info().ShortDebugString();
}

void Group::RemoveMember(const std::string& member_id) {
  id2member_.erase(member_id);
  LOG(INFO) << "Remove member from group success: group_name = " << group_name_
            << ", member_id = " << member_id;
}

std::vector<MemberSPtr> Group::GetAllMembers() {
  std::vector<MemberSPtr> members;
  members.reserve(id2member_.size());
  for (const auto& item : id2member_) {
    members.emplace_back(item.second);
  }
  return members;
}

// groups
Groups::Groups(kvstorage::KVStorageClientSPtr storage) : storage_(storage) {}

Status Groups::Load() {
  CHECK_NOTNULL(storage_);
  CHECK(name2group_.empty());

  std::vector<std::pair<std::string, std::string>> kvs;
  std::string start = CACHE_GROUP_GROUP_NAME_KEY_PREFIX;
  std::string end = CACHE_GROUP_GROUP_NAME_KEY_END;
  int rc = storage_->List(start, end, &kvs);
  if (rc != EtcdErrCode::EtcdOK) {
    LOG(ERROR) << "List group names from storage failed: rc = " << rc;
    return Status::Internal("list group names failed");
  }

  for (const auto& kv : kvs) {
    auto group_name = Codec::DecodeGroupName(kv.first);
    auto birth_time = Codec::DecodeGroupBirthTime(kv.second);
    auto group = std::make_shared<Group>(group_name);
    name2group_.emplace(group_name, group);
    LOG(INFO) << "Load one group success: group_name = " << group_name
              << ", birth_time = " << kv.second;
  }

  LOG(INFO) << name2group_.size() << " groups loaded.";
  return Status::OK();
}

Status Groups::GetGroup(const std::string& group_name, GroupSPtr& group) {
  auto iter = name2group_.find(group_name);
  if (iter != name2group_.end()) {
    group = iter->second;
    return Status::OK();
  }
  return Status::NotFound("group not found");
}

Status Groups::RegisterGroup(const std::string& group_name, GroupSPtr& group) {
  auto status = CheckGroupName(group_name);
  if (!status.ok()) {
    return status;
  }

  auto iter = name2group_.find(group_name);
  if (iter != name2group_.end()) {
    group = iter->second;
    return Status::Exist("group already exists");
  }

  status = StoreGroupName(group_name);
  if (status.ok()) {
    group = std::make_shared<Group>(group_name);
    name2group_[group_name] = group;
  }
  return status;
}

std::vector<std::string> Groups::GetAllGroupNames() {
  std::vector<std::string> group_names;
  group_names.reserve(name2group_.size());
  for (const auto& item : name2group_) {
    group_names.emplace_back(item.first);
  }
  return group_names;
}

Status Groups::CheckGroupName(const std::string& group_name) {
  if (group_name.empty()) {
    return Status::InvalidParam("group name is empty");
  } else if (group_name.size() > 255) {
    return Status::InvalidParam("group name is too long");
  }
  return Status::OK();
}

Status Groups::StoreGroupName(const std::string& group_name) {
  std::string key = Codec::EncodeGroupName(group_name);
  std::string value = Codec::EncodeGroupBirthTime(Helper::TimestampMs());
  auto rc = storage_->Put(key, value);
  if (rc != EtcdErrCode::EtcdOK) {
    LOG(ERROR) << "Store group name failed: group_name = " << group_name
               << ", rc = " << rc;
    return Status::Internal("store group name failed");
  }
  return Status::OK();
}

}  // namespace cachegroup
}  // namespace mds
}  // namespace dingofs
