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
 * Created Date: 2025-02-06
 * Author: Jingli Chen (Wine93)
 */

#include "mds/cachegroup/cache_group_member_storage.h"

#include <glog/logging.h>

#include <memory>
#include <string>

#include "common/status.h"
#include "mds/cachegroup/codec.h"
#include "mds/cachegroup/common.h"
#include "mds/cachegroup/config.h"
#include "mds/cachegroup/helper.h"
#include "mds/common/storage_key.h"
#include "utils/concurrent/concurrent.h"
#include "utils/string.h"

namespace dingofs {
namespace mds {
namespace cachegroup {

// member
Member::Member(const PBCacheGroupMember& member_info,
               kvstorage::KVStorageClientSPtr storage)
    : member_info_(member_info),
      storage_(storage),
      group_(std::make_shared<Group>("")) {}

Status Member::JoinCacheGroup(GroupSPtr group, uint32_t weight) {
  utils::WriteLockGuard lock(rwlock_);
  if (member_info_.group_name() == group->Name() &&
      member_info_.weight() == weight) {
    return Status::OK();
  } else if (member_info_.id() == 0) {
    LOG(ERROR) << "The member has been replaced, cannot join group: member = "
               << member_info_.DebugString();
    return Status::Internal("member has been replaced");
  }

  member_info_.set_weight(weight);
  member_info_.set_group_name(group->Name());
  auto status = Store();
  if (!status.ok()) {
    return status;
  }

  group_ = group;
  group->AddMember(shared_from_this());
  return Status::OK();
}

Status Member::LeaveCacheGroup() {
  utils::WriteLockGuard lock(rwlock_);
  if (!member_info_.has_group_name()) {
    return Status::OK();
  }

  member_info_.clear_group_name();
  auto status = Store();
  if (status.ok()) {
    group_->RemoveMember(member_info_.id());
  }
  return status;
}

Status Member::Heartbeat(uint64_t last_online_time_ms) {
  utils::WriteLockGuard lock(rwlock_);
  member_info_.set_last_online_time_ms(last_online_time_ms);
  return Store();
}

Status Member::Reweight(uint32_t weight) {
  utils::WriteLockGuard lock(rwlock_);
  member_info_.set_weight(weight);
  return Store();
}

Status Member::Store() {
  auto key = Codec::EncodeMemberId(member_info_.id());
  auto value = Codec::EncodeMember(member_info_);
  int rc = storage_->Put(key, value);
  if (rc != EtcdErrCode::EtcdOK) {
    LOG(ERROR) << "Store member failed: id = " << member_info_.id()
               << ", rc = " << rc;
    return Status::IoError("store member failed");
  }

  VLOG(3) << "Store member success: " << member_info_.ShortDebugString();
  return Status::OK();
}

Status Member::Destroy() {
  utils::WriteLockGuard lock(rwlock_);
  if (GetState() != PBCacheGroupMemberState::CacheGroupMemberStateOffline) {
    return Status::InvalidParam("member is not offline");
  }

  auto member_id = member_info_.id();
  member_info_.set_id(0);
  member_info_.clear_group_name();
  auto status = Store();
  if (status.ok()) {
    group_->RemoveMember(member_id);
  }
  return Status::OK();
}

PBCacheGroupMember Member::GetInfo() {
  utils::ReadLockGuard lock(rwlock_);
  member_info_.set_state(GetState());
  return member_info_;
}

PBCacheGroupMemberState Member::GetState() const {
  auto time_now_ms = Helper::TimestampMs();
  auto miss_timeout_ms = FLAGS_heartbeat_miss_timeout_s * 1000;
  auto offline_timeout_ms = FLAGS_heartbeat_offline_timeout_s * 1000;

  auto time_pass_ms = time_now_ms - member_info_.last_online_time_ms();
  if (time_pass_ms < miss_timeout_ms) {
    return PBCacheGroupMemberState::CacheGroupMemberStateOnline;
  } else if (time_pass_ms < offline_timeout_ms) {
    return PBCacheGroupMemberState::CacheGroupMemberStateUnstable;
  }
  return PBCacheGroupMemberState::CacheGroupMemberStateOffline;
}

// members
Members::Members(kvstorage::KVStorageClientSPtr storage) : storage_(storage) {}

Status Members::Load() {
  std::vector<std::pair<std::string, std::string>> kvs;
  std::string start = CACHE_GROUP_MEMBER_ID_KEY_PREFIX;
  std::string end = CACHE_GROUP_MEMBER_ID_KEY_PREFIX;
  int rc = storage_->List(start, end, &kvs);
  if (rc != EtcdErrCode::EtcdOK) {
    LOG(ERROR) << "List members from storage failed: rc = " << rc;
    return Status::IoError("list members failed");
  }

  for (const auto& kv : kvs) {
    auto member_id = Codec::DecodeMemberId(kv.first);
    auto member_info = Codec::DecodeMember(kv.second);
    auto endpoint = Helper::EndPoint(member_info.ip(), member_info.port());
    auto member = std::make_shared<Member>(member_info, storage_);
    if (member_info.id() != member_id) {
      LOG(ERROR) << "Member meybe repalced: member_id = " << member_id
                 << ", member = " << member_info.ShortDebugString();
    }

    endpoint2id_.emplace(endpoint, member_id);
    id2member_.emplace(member_id, member);
    LOG(INFO) << "Load member success: member = "
              << member_info.ShortDebugString();
  }

  LOG(INFO) << id2member_.size() << " members loaded.";
  return Status::OK();
}

Status Members::GetMember(const std::string& ip, uint32_t port,
                          MemberSPtr& member) {
  utils::ReadLockGuard lock(rwlock_);
  auto endpoint = Helper::EndPoint(ip, port);
  if (endpoint2id_.count(endpoint) == 0) {
    return Status::NotFound("member not found");
  }

  auto iter = id2member_.find(endpoint2id_[endpoint]);
  if (iter == id2member_.end()) {
    return Status::NotFound("member not found");
  }
  member = iter->second;
  return Status::OK();
}

Status Members::GetMember(uint64_t member_id, MemberSPtr& member) {
  utils::ReadLockGuard lock(rwlock_);
  auto iter = id2member_.find(member_id);
  if (iter == id2member_.end()) {
    return Status::NotFound("member not found");
  }
  member = iter->second;
  return Status::OK();
}

Status Members::CreateMember(const std::string& ip, uint32_t port,
                             MemberSPtr& member) {
  utils::WriteLockGuard lock(rwlock_);
  auto endpoint = Helper::EndPoint(ip, port);
  if (endpoint2id_.count(endpoint) != 0) {
    member = id2member_[endpoint2id_[endpoint]];
    return Status::Exist("member already exists");
  }

  uint64_t member_id;
  if (member_id_generator_->GenId(1, &member_id) != 0) {
    LOG(ERROR) << "Generate member id failed: ip = " << ip
               << ", port = " << port;
    return Status::IoError("generate member id failed");
  }

  auto member_info = NewMemberInfo(member_id, ip, port);
  member = std::make_shared<Member>(member_info, storage_);
  auto status = member->Store();
  if (!status.ok()) {
    return status;
  }

  endpoint2id_.emplace(endpoint, member_id);
  id2member_.emplace(member_id, member);
  return Status::OK();
}

Status Members::ReplaceMember(uint64_t member_id, const std::string& ip,
                              uint32_t port, MemberSPtr& member) {
  utils::WriteLockGuard lock(rwlock_);
  auto iter = id2member_.find(member_id);
  if (iter == id2member_.end()) {
    return Status::NotFound("member not found");
  }

  // Check if the new endpoint is the same as the old one
  auto old_member = iter->second;
  auto old_info = old_member->GetInfo();
  const auto& old_ip = old_info.ip();
  auto old_port = old_info.port();
  if (old_ip == ip && old_port == port) {
    member = old_member;
    return Status::OK();
  }

  // Destroy the old member
  auto status = old_member->Destroy();
  if (!status.ok()) {
    LOG(INFO) << "Destroy old member failed: old_member = "
              << old_info.DebugString() << ", status = " << status.ToString();
    return status;
  }

  // Create a new member with the new endpoint
  auto member_info = NewMemberInfo(member_id, ip, port);
  member = std::make_shared<Member>(member_info, storage_);
  status = member->Store();
  if (!status.ok()) {
    LOG(ERROR) << "Store new member failed: new_member = "
               << member_info.DebugString()
               << ", status = " << status.ToString();
    return status;
  }

  // Update the endpoint2id_ and id2member_ maps
  auto old_endpoint = Helper::EndPoint(old_ip, old_port);
  auto new_endpoint = Helper::EndPoint(ip, port);
  endpoint2id_.erase(old_endpoint);
  endpoint2id_[new_endpoint] = member_id;
  id2member_[member_id] = member;

  return Status::OK();
}

std::vector<MemberSPtr> Members::GetAllMembers() {
  utils::ReadLockGuard lock(rwlock_);
  std::vector<MemberSPtr> members;
  members.reserve(id2member_.size());
  for (const auto& item : id2member_) {
    members.emplace_back(item.second);
  }
  return members;
}

PBCacheGroupMember Members::NewMemberInfo(uint64_t member_id,
                                          const std::string& ip,
                                          uint32_t port) {
  PBCacheGroupMember member_info;
  member_info.set_id(member_id);
  member_info.set_uuid(utils::GenUuid());
  member_info.set_ip(ip);
  member_info.set_port(port);
  member_info.set_weight(0);
  member_info.set_last_online_time_ms(0);
  member_info.set_state(PBCacheGroupMemberState::CacheGroupMemberStateOffline);
  return member_info;
}

// group
Group::Group(const std::string& group_name) : group_name_(group_name) {}

void Group::AddMember(MemberSPtr member) {
  utils::WriteLockGuard lock(rwlock_);
  uint64_t member_id = member->GetInfo().id();
  id2member_[member_id] = member;
}

void Group::RemoveMember(uint64_t member_id) {
  utils::WriteLockGuard lock(rwlock_);
  id2member_.erase(member_id);
}

std::vector<MemberSPtr> Group::GetAllMembers() {
  utils::ReadLockGuard lock(rwlock_);
  std::vector<MemberSPtr> members;
  members.reserve(id2member_.size());
  for (const auto& item : id2member_) {
    members.emplace_back(item.second);
  }
  return members;
}

size_t Group::Size() {
  utils::ReadLockGuard lock(rwlock_);
  return id2member_.size();
}
std::string Group::Name() const { return group_name_; }

// groups
Groups::Groups(kvstorage::KVStorageClientSPtr storage) : storage_(storage) {}

Status Groups::Load() {
  std::vector<std::pair<std::string, std::string>> kvs;
  std::string start = CACHE_GROUP_GROUP_NAME_KEY_PREFIX;
  std::string end = CACHE_GROUP_GROUP_NAME_KEY_END;
  int rc = storage_->List(start, end, &kvs);
  if (rc != EtcdErrCode::EtcdOK) {
    LOG(ERROR) << "List group names from storage failed: rc = " << rc;
    return Status::IoError("list group names failed");
  }

  for (const auto& kv : kvs) {
    auto group_name = Codec::DecodeGroupName(kv.first);
    auto birth_time = Codec::DecodeGroupBirthTime(kv.second);
    auto group = std::make_shared<Group>(group_name);
    name2group_.emplace(group_name, group);
    LOG(INFO) << "Load group: " << group_name << ", birth_time = " << kv.second;
  }

  LOG(INFO) << name2group_.size() << " groups loaded.";
  return Status::OK();
}

Status Groups::GetGroup(const std::string& group_name, GroupSPtr& group) {
  utils::ReadLockGuard lock(rwlock_);
  if (name2group_.count(group_name) != 0) {
    group = name2group_[group_name];
    return Status::OK();
  }
  return Status::NotFound("group not found");
}

Status Groups::CreateGroup(const std::string& group_name, GroupSPtr& group) {
  utils::WriteLockGuard lock(rwlock_);
  auto status = CheckGroupName(group_name);
  if (!status.ok()) {
    return status;
  }

  if (name2group_.count(group_name) != 0) {
    group = name2group_[group_name];
    return Status::Exist("group already exists");
  }

  status = StoreGroupName(group_name);
  if (status.ok()) {
    name2group_[group_name] = std::make_shared<Group>(group_name);
  }
  return status;
}

std::vector<std::string> Groups::GetAllGroupNames() {
  utils::ReadLockGuard lock(rwlock_);
  std::vector<std::string> group_names;
  group_names.reserve(name2group_.size());
  for (const auto& item : name2group_) {
    group_names.emplace_back(item.second->Name());
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
    return Status::IoError("store group name failed");
  }
  return Status::OK();
}

}  // namespace cachegroup
}  // namespace mds
}  // namespace dingofs
