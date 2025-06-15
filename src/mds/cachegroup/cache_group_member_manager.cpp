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

#include "mds/cachegroup/errno.h"

namespace dingofs {
namespace mds {
namespace cachegroup {

CacheGroupMemberManagerImpl::CacheGroupMemberManagerImpl(
    CacheGroupOption option, std::shared_ptr<KVStorageClient> kv)
    : option_(option),
      storage_(std::make_unique<CacheGroupMemberStorageImpl>(kv)) {}

bool CacheGroupMemberManagerImpl::Init() { return storage_->Init(); }

bool CacheGroupMemberManagerImpl::CheckId(uint64_t /*member_id*/) {
  // TODO(Wine93): check id boundaries
  return true;
}

Errno CacheGroupMemberManagerImpl::RegisterMember(uint64_t old_id,
                                                  uint64_t* member_id) {
  if (old_id == 0) {
    return storage_->RegisterMember(member_id);
  } else if (!CheckId(old_id)) {
    return Errno::kInvalidMemberId;
  }
  *member_id = old_id;
  return Errno::kOk;
}

Errno CacheGroupMemberManagerImpl::AddMember(const std::string& group_name,
                                             const CacheGroupMember& member) {
  uint64_t group_id;
  auto rc = storage_->GetGroupId(group_name, &group_id);
  if (rc == Errno::kNotFound) {
    rc = storage_->RegisterGroup(group_name, &group_id);
    if (rc != Errno::kOk) {
      LOG(ERROR) << "Register group (name=" << group_name
                 << ") failed: " << StrErr(rc);
      return rc;
    }

    LOG(INFO) << "Register group (name=" << group_name
              << ") success: group_id = " << group_id;
  }

  // rc == Errno::kOk
  rc = storage_->AddMember(group_id, member);
  if (rc == Errno::kOk) {
    LOG(INFO) << "Add member (id=" << member.id()
              << ",weight=" << member.weight()
              << ") to group (name=" << group_name << ") success.";
  } else {
    LOG(ERROR) << "Add member (id=" << member.id()
               << ",weight=" << member.weight()
               << ") to group (name=" << group_name
               << ") failed: " << StrErr(rc);
  }
  return rc;
}

Errno CacheGroupMemberManagerImpl::LoadMembers(
    const std::string& group_name, std::vector<CacheGroupMember>* members) {
  uint64_t group_id;
  auto rc = storage_->GetGroupId(group_name, &group_id);
  if (rc == Errno::kOk) {
    storage_->LoadMembers(group_id, members);
  }
  return rc;
}

Errno CacheGroupMemberManagerImpl::ReweightMember(const std::string& group_name,
                                                  uint64_t member_id,
                                                  uint32_t weight) {
  uint64_t group_id;
  auto rc = storage_->GetGroupId(group_name, &group_id);
  if (rc == Errno::kOk) {
    if (CheckId(member_id)) {
      rc = storage_->ReweightMember(group_id, member_id, weight);
    } else {
      rc = Errno::kFail;
    }
  }
  return rc;
}

Errno CacheGroupMemberManagerImpl::HandleHeartbeat(
    const std::string& /*group_name*/, uint64_t /*id*/,
    const Statistic& /*stat*/) {
  // TODO(Wine93): handle heartbeat, use CacheGroupOption
  return Errno::kOk;
}

}  // namespace cachegroup
}  // namespace mds
}  // namespace dingofs
