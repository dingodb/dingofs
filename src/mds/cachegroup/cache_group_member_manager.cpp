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

using ::dingofs::pb::mds::cachegroup::CacheGroupMemberState;

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
                                             CacheGroupMember member) {
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
  member.set_last_online_time_ms(TimestampMs());
  rc = storage_->AddMember(group_id, member);
  if (rc == Errno::kOk) {
    LOG(INFO) << "Add member (id=" << member.id() << ",ip=" << member.ip()
              << ",port=" << member.port() << ",weight=" << member.weight()
              << ",last_online_time_ms=" << member.last_online_time_ms()
              << ",state=" << member.state()
              << ") to group (name=" << group_name << ",id=" << group_id
              << ") success.";
  } else {
    LOG(ERROR) << "Add member (id=" << member.id() << ",ip=" << member.ip()
               << ",port=" << member.port() << ",weight=" << member.weight()
               << ",last_online_time_ms=" << member.last_online_time_ms()
               << ",state=" << member.state()
               << ") to group (name=" << group_name << ",id=" << group_id
               << ") failed: " << StrErr(rc);
  }
  return rc;
}

void CacheGroupMemberManagerImpl::SetMembersState(
    std::vector<CacheGroupMember>* members) {
  auto time_now_ms = TimestampMs();
  auto miss_timeout_ms = option_.heartbeat_miss_timeout_s * 1000;
  auto offline_timeout_ms = option_.heartbeat_offline_timeout_s * 1000;

  auto set_state = [&](CacheGroupMember* member) {
    auto time_pass_ms = time_now_ms - member->last_online_time_ms();
    if (time_pass_ms < miss_timeout_ms) {
      member->set_state(CacheGroupMemberState::CacheGroupMemberStateOnline);
    } else if (time_pass_ms < offline_timeout_ms) {
      member->set_state(CacheGroupMemberState::CacheGroupMemberStateUnstable);
    } else {  // TODO: we can remove offline members from group
      member->set_state(CacheGroupMemberState::CacheGroupMemberStateOffline);
    }
  };

  for (auto& member : *members) {
    set_state(&member);
  }
}

void CacheGroupMemberManagerImpl::FilterOutOfflineMember(
    const std::vector<CacheGroupMember>& members,
    std::vector<CacheGroupMember>* members_out) {
  for (const auto& member : members) {
    if (member.state() != CacheGroupMemberState::CacheGroupMemberStateOffline) {
      members_out->emplace_back(member);
    }
  }
}

Errno CacheGroupMemberManagerImpl::LoadMembers(
    const std::string& group_name, std::vector<CacheGroupMember>* members_out) {
  uint64_t group_id;
  auto rc = storage_->GetGroupId(group_name, &group_id);
  if (rc != Errno::kOk) {
    return rc;
  }

  std::vector<CacheGroupMember> members;
  storage_->LoadMembers(group_id, &members);
  SetMembersState(&members);
  FilterOutOfflineMember(members, members_out);

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
    const std::string& group_name, uint64_t member_id,
    const Statistic& /*stat*/) {
  uint64_t group_id;
  auto rc = storage_->GetGroupId(group_name, &group_id);
  if (rc != Errno::kOk) {
    LOG(ERROR) << "Group not found: group_name = " << group_name;
    return rc;
  }

  return storage_->SetMemberLastOnlineTime(group_id, member_id, TimestampMs());
}

std::vector<std::string> CacheGroupMemberManagerImpl::LoadGroups() {
  return storage_->GetGroups();
}

uint64_t CacheGroupMemberManagerImpl::TimestampMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

}  // namespace cachegroup
}  // namespace mds
}  // namespace dingofs
