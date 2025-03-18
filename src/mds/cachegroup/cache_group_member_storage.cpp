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

#include "mds/common/storage_key.h"
#include "utils/concurrent/concurrent.h"
#include "utils/encode.h"

namespace dingofs {
namespace mds {
namespace cachegroup {

using ::dingofs::mds::CACHE_GROUP_GROUP_ID_KEY_PREFIX;
using ::dingofs::mds::CACHE_GROUP_GROUP_MEMBER_KEY_PREFIX;
using ::dingofs::mds::CACHE_GROUP_GROUP_NAME_KEY_PREFIX;
using ::dingofs::mds::CACHE_GROUP_MEMBER_ID_KEY_PREFIX;
using ::dingofs::mds::CACHE_GROUP_PREFIX_LENGTH;
using ::dingofs::utils::DecodeBigEndian;
using ::dingofs::utils::EncodeBigEndian;
using ::dingofs::utils::ReadLockGuard;
using ::dingofs::utils::WriteLockGuard;

class Helper {
 public:
  static std::string EncodeGroupNameKey(uint64_t group_id) {
    std::string key = CACHE_GROUP_GROUP_NAME_KEY_PREFIX;
    size_t prefix_len = CACHE_GROUP_PREFIX_LENGTH;
    key.resize(prefix_len + sizeof(uint64_t));
    EncodeBigEndian(&(key[prefix_len]), group_id);
    return key;
  }

  static void DecodeGroupNameKey(const std::string& key, uint64_t* group_id) {
    size_t prefix_len = CACHE_GROUP_PREFIX_LENGTH;
    *group_id = DecodeBigEndian(&(key[prefix_len]));
  }

  static std::string EncodeGroupMemberKey(uint64_t group_id,
                                          uint64_t member_id) {
    std::string key = CACHE_GROUP_GROUP_MEMBER_KEY_PREFIX;
    size_t prefix_len = CACHE_GROUP_PREFIX_LENGTH;
    key.resize(prefix_len + sizeof(uint64_t) + sizeof(uint64_t));
    EncodeBigEndian(&(key[prefix_len]), group_id);
    EncodeBigEndian(&(key[prefix_len + sizeof(uint64_t)]), member_id);
    return key;
  }

  static void DecodeGroupMemberKey(const std::string& key, uint64_t* group_id,
                                   uint64_t* member_id) {
    size_t prefix_len = CACHE_GROUP_PREFIX_LENGTH;
    *group_id = DecodeBigEndian(&(key[prefix_len]));
    *member_id = DecodeBigEndian(&(key[prefix_len + sizeof(uint64_t)]));
  }
};

CacheGroupMemberStorageImpl::CacheGroupMemberStorageImpl(
    std::shared_ptr<KVStorageClient> kv)
    : kv_(kv),
      member_id_generator_(std::make_unique<EtcdIdGenerator>(
          kv, CACHE_GROUP_MEMBER_ID_KEY_PREFIX, 1, 100)),
      group_id_generator_(std::make_unique<EtcdIdGenerator>(
          kv, CACHE_GROUP_GROUP_ID_KEY_PREFIX, 1, 100)) {}

bool CacheGroupMemberStorageImpl::Init() {
  return LoadGroupNames() && LoadGroupMembers();
}

bool CacheGroupMemberStorageImpl::LoadGroupNames() {
  std::vector<std::pair<std::string, std::string>> kvs;
  std::string start = CACHE_GROUP_GROUP_NAME_KEY_PREFIX;
  std::string end = CACHE_GROUP_GROUP_NAME_KEY_END;
  int rc = kv_->List(start, end, &kvs);
  if (rc != 0) {
    LOG(ERROR) << "Load group names failed, rc=" << rc;
    return false;
  }

  uint64_t group_id;
  for (const auto& kv : kvs) {
    Helper::DecodeGroupNameKey(kv.first, &group_id);
    group_names_.emplace(kv.second, group_id);
  }
  LOG(INFO) << group_names_.size() << " group names loaded.";
  return true;
}

bool CacheGroupMemberStorageImpl::LoadGroupMembers() {
  std::vector<std::pair<std::string, std::string>> kvs;
  std::string start = CACHE_GROUP_GROUP_MEMBER_KEY_PREFIX;
  std::string end = CACHE_GROUP_GROUP_MEMBER_KEY_END;
  int rc = kv_->List(start, end, &kvs);
  if (rc != 0) {
    LOG(ERROR) << "Load group members failed, rc=" << rc;
    return false;
  }

  uint64_t group_id, member_id, num_members = 0;
  CacheGroupMember member;
  for (const auto& kv : kvs) {
    Helper::DecodeGroupMemberKey(kv.first, &group_id, &member_id);
    if (!member.ParseFromString(kv.second)) {
      LOG(ERROR) << "Parse group member failed, key=" << kv.first;
      return false;
    }
    AddMember2Group(group_id, member_id, member);
    num_members++;
  }
  LOG(INFO) << groups_.size() << " groups " << num_members
            << " members loaded.";
  return true;
}

void CacheGroupMemberStorageImpl::AddMember2Group(
    uint64_t group_id, uint64_t member_id, const CacheGroupMember& member) {
  auto iter = groups_.find(group_id);
  if (iter == groups_.end()) {
    iter = groups_.emplace(group_id, CacheGroupMembersType()).first;
  }
  auto& members = iter->second;
  members.emplace(member_id, member);
}

bool CacheGroupMemberStorageImpl::StoreGroupName(
    uint64_t group_id, const std::string& group_name) {
  std::string key = Helper::EncodeGroupNameKey(group_id);
  return kv_->Put(key, group_name) == 0;
}

bool CacheGroupMemberStorageImpl::StoreGroupMember(
    uint64_t group_id, uint64_t member_id, const CacheGroupMember& member) {
  std::string value;
  std::string key = Helper::EncodeGroupMemberKey(group_id, member_id);
  if (member.SerializeToString(&value)) {
    return kv_->Put(key, value) == 0;
  }
  return false;
}

Errno CacheGroupMemberStorageImpl::GetGroupId(const std::string& group_name,
                                              uint64_t* id) {
  ReadLockGuard lk(rwlock_);
  auto iter = group_names_.find(group_name);
  if (iter == group_names_.end()) {
    return Errno::kInvalidGroupName;
  }
  *id = iter->second;
  return Errno::kOk;
}

Errno CacheGroupMemberStorageImpl::RegisterMember(uint64_t* member_id) {
  WriteLockGuard lk(rwlock_);
  bool succ = member_id_generator_->GenID(member_id);
  return succ ? Errno::kOk : Errno::kFail;
}

Errno CacheGroupMemberStorageImpl::RegisterGroup(const std::string& group_name,
                                                 uint64_t* group_id) {
  WriteLockGuard lk(rwlock_);
  if (group_names_.count(group_name) != 0) {
    *group_id = group_names_[group_name];
    return Errno::kOk;
  }

  bool succ = member_id_generator_->GenID(group_id);
  if (succ) {
    succ = StoreGroupName(*group_id, group_name);
    if (succ) {
      group_names_[group_name] = *group_id;
      return Errno::kOk;
    }
  }
  return Errno::kFail;
}

Errno CacheGroupMemberStorageImpl::AddMember(uint64_t group_id,
                                             const CacheGroupMember& member) {
  WriteLockGuard lk(rwlock_);
  uint64_t member_id = member.id();
  if (StoreGroupMember(group_id, member_id, member)) {
    AddMember2Group(group_id, member_id, member);
    return Errno::kOk;
  }
  return Errno::kFail;
}

void CacheGroupMemberStorageImpl::LoadMembers(
    uint64_t group_id, std::vector<CacheGroupMember>* members) {
  members->clear();

  ReadLockGuard lk(rwlock_);
  auto iter = groups_.find(group_id);
  if (iter != groups_.end()) {
    auto& group_members = iter->second;
    for (const auto& item : group_members) {
      members->emplace_back(item.second);
    }
  }
}

Errno CacheGroupMemberStorageImpl::ReweightMember(uint64_t group_id,
                                                  uint64_t member_id,
                                                  uint32_t weight) {
  WriteLockGuard lk(rwlock_);
  auto iter = groups_.find(group_id);
  if (iter == groups_.end()) {
    return Errno::kInvalidGroupName;
  }

  auto& members = iter->second;
  if (members.count(member_id) == 0) {
    return Errno::kInvalidMemberId;
  }

  auto member = members[member_id];
  member.set_weight(weight);
  if (StoreGroupMember(group_id, member_id, member)) {
    members[member_id] = member;
    return Errno::kOk;
  }
  return Errno::kFail;
}

}  // namespace cachegroup
}  // namespace mds
}  // namespace dingofs
