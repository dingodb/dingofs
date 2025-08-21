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

#include "mds/cachegroup/member.h"

#include <glog/logging.h>

#include <memory>
#include <ostream>

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

using utils::ReadLockGuard;
using utils::WriteLockGuard;

Member::Member(const PBCacheGroupMember& info,
               kvstorage::KVStorageClientSPtr storage)
    : info_(info), storage_(storage) {}

Status Member::SetGroup(const std::string& group_name, uint32_t weight) {
  return DoStore([group_name, weight](PBCacheGroupMember* info) {
    info->set_group_name(group_name);
    info->set_weight(weight);
    return Status::OK();
  });
}

Status Member::SetLastOnlineTime(uint64_t last_online_time_ms) {
  return DoStore([last_online_time_ms](PBCacheGroupMember* info) {
    info->set_last_online_time_ms(last_online_time_ms);
    return Status::OK();
  });
}

Status Member::SetWeight(uint32_t weight) {
  return DoStore([weight](PBCacheGroupMember* info) {
    info->set_weight(weight);
    return Status::OK();
  });
}

Status Member::ClearGroup() {
  return DoStore([](PBCacheGroupMember* info) {
    info->clear_group_name();
    return Status::OK();
  });
}

Status Member::ClearAll() {
  return DoStore([this](PBCacheGroupMember* info) {
    if (GetState() != PBCacheGroupMemberState::CacheGroupMemberStateOffline) {
      LOG(WARNING) << "Clear all member info, but member is still online: "
                   << info->ShortDebugString();
      return Status::InvalidParam("member is not offline");
    }

    info->set_ip("");
    info->set_port(0);
    info->set_weight(0);
    info->set_last_online_time_ms(0);
    info->set_state(PBCacheGroupMemberState::CacheGroupMemberStateUnknown);
    info->clear_group_name();

    return Status::OK();
  });
}

Status Member::Store() {
  return DoStore([](PBCacheGroupMember*) { return Status::OK(); });
}

Status Member::DoStore(UpdateFn fn) {
  WriteLockGuard lock(rwlock_);
  if (IsDeregisterd()) {
    return Status::InvalidParam("member is deregistered");
  }

  auto old_info = info_;
  auto status = fn(&info_);
  if (status.ok()) {
    status = Store(info_);
  }

  if (!status.ok()) {
    info_ = old_info;
  }
  return status;
}

Status Member::Store(const PBCacheGroupMember& info) {
  auto key = Codec::EncodeMemberId(info.id());
  auto value = Codec::EncodeMember(info);
  int rc = storage_->Put(key, value);
  if (rc != EtcdErrCode::EtcdOK) {
    LOG(ERROR) << "Store member failed: id = " << info.id()
               << ", member = " << info.ShortDebugString() << ", rc = " << rc;
    return Status::Internal("store member failed");
  }

  VLOG(3) << "Store member success: member = " << info.ShortDebugString();
  return Status::OK();
}

PBCacheGroupMember Member::Info() {
  ReadLockGuard lock(rwlock_);
  auto info = info_;
  info.set_state(GetState());
  return info;
}

PBCacheGroupMemberState Member::GetState() const {
  if (IsDeregisterd()) {
    return PBCacheGroupMemberState::CacheGroupMemberStateUnknown;
  }

  auto time_now_ms = Helper::TimestampMs();
  auto miss_timeout_ms = FLAGS_heartbeat_miss_timeout_s * 1000;
  auto offline_timeout_ms = FLAGS_heartbeat_offline_timeout_s * 1000;

  auto time_pass_ms = time_now_ms - info_.last_online_time_ms();
  if (time_pass_ms < miss_timeout_ms) {
    return PBCacheGroupMemberState::CacheGroupMemberStateOnline;
  } else if (time_pass_ms < offline_timeout_ms) {
    return PBCacheGroupMemberState::CacheGroupMemberStateUnstable;
  }
  return PBCacheGroupMemberState::CacheGroupMemberStateOffline;
}

bool Member::IsDeregisterd() const {
  return info_.ip().empty() && info_.port() == 0;
}

// members
Members::Members(kvstorage::KVStorageClientSPtr storage) : storage_(storage) {}

Status Members::Load() {
  CHECK_NOTNULL(storage_);
  CHECK(idset_.empty());
  CHECK(endpoint2id_.empty());
  CHECK(id2member_.empty());

  std::vector<std::pair<std::string, std::string>> kvs;
  std::string start = CACHE_GROUP_MEMBER_ID_KEY_PREFIX;
  std::string end = CACHE_GROUP_MEMBER_ID_KEY_END;
  int rc = storage_->List(start, end, &kvs);
  if (rc != EtcdErrCode::EtcdOK) {
    LOG(ERROR) << "List members from storage failed: rc = " << rc;
    return Status::Internal("list members failed");
  }

  for (const auto& kv : kvs) {
    auto member_id = Codec::DecodeMemberId(kv.first);
    auto ret = idset_.emplace(member_id);
    CHECK(ret.second) << "Member id already exists: member_id = " << member_id;

    auto member_info = Codec::DecodeMember(kv.second);
    auto endpoint = EndPoint(member_info.ip(), member_info.port());
    auto member = std::make_shared<Member>(member_info, storage_);
    if (member->IsDeregisterd()) {
      LOG(INFO) << "Load deregistered member, ignore it: member_id = "
                << member_id << ", member = " << member_info.ShortDebugString();
      continue;
    }

    endpoint2id_.emplace(endpoint.ToString(), member_id);
    id2member_.emplace(member_id, member);
    LOG(INFO) << "Load member success: member = "
              << member_info.ShortDebugString();
  }

  LOG(INFO) << id2member_.size() << " members loaded.";
  return Status::OK();
}

Status Members::GetMember(const EndPoint& endpoint, MemberSPtr& member) {
  if (endpoint2id_.count(endpoint.ToString()) == 0) {
    return Status::NotFound("member not found");
  }

  auto iter = id2member_.find(endpoint2id_[endpoint.ToString()]);
  if (iter != id2member_.end()) {
    member = iter->second;
    return Status::OK();
  }
  return Status::NotFound("member not found");
}

Status Members::GetMember(const std::string& member_id, MemberSPtr& member) {
  auto iter = id2member_.find(member_id);
  if (iter != id2member_.end()) {
    member = iter->second;
    return Status::OK();
  }
  return Status::NotFound("member not found");
}

Status Members::RegisterMember(const EndPoint& endpoint, MemberSPtr& member) {
  auto ep = endpoint.ToString();
  if (endpoint2id_.count(ep) != 0) {
    member = id2member_[endpoint2id_[ep]];
    return Status::Exist("member already exists");
  }

  auto id = utils::GenUuid();
  auto info = NewMemberInfo(id, endpoint, Helper::Timestamp());
  member = std::make_shared<Member>(info, storage_);
  auto status = member->Store();
  if (!status.ok()) {
    return status;
  }

  idset_.insert(id);
  endpoint2id_.emplace(ep, id);
  id2member_.emplace(id, member);

  LOG(INFO) << "Register member with auto id success: endpoint = " << ep
            << ", member_id = " << id
            << ", member = " << info.ShortDebugString();

  return Status::OK();
}

// RegisterMember with a specific id, MUST gurantee:
// 1. The endpoint is not registered.
// 2. The wanted id is exist.
// 3. The wanted id is not registered.
Status Members::RegisterMember(const EndPoint& endpoint,
                               const std::string& want_id, MemberSPtr& member) {
  auto ep = endpoint.ToString();
  if (endpoint2id_.count(ep) != 0) {
    if (endpoint2id_[ep] == want_id) {
      member = id2member_[want_id];
      return Status::Exist("member already exists");
    }
    return Status::InvalidParam("endpoint already registered");
  } else if (idset_.count(want_id) == 0) {
    return Status::InvalidParam("wanted id not exist");
  } else if (id2member_.count(want_id) != 0) {
    return Status::InvalidParam("wanted id already registered");
  }

  auto info = NewMemberInfo(want_id, endpoint, Helper::Timestamp());
  member = std::make_shared<Member>(info, storage_);
  auto status = member->Store();
  if (!status.ok()) {
    return status;
  }

  endpoint2id_.emplace(ep, want_id);
  id2member_.emplace(want_id, member);

  LOG(INFO) << "Register member with want id success: endpoint = "
            << endpoint.ToString() << ", want_id = " << want_id
            << ", member = " << info.ShortDebugString();
  return Status::OK();
}

Status Members::DeregisterMember(const EndPoint& endpoint,
                                 PBCacheGroupMember* old_info) {
  auto ep = endpoint.ToString();
  if (endpoint2id_.count(ep) == 0) {
    return Status::NotFound("member not found");
  }

  auto iter = id2member_.find(endpoint2id_[ep]);
  if (iter == id2member_.end()) {
    return Status::NotFound("member not found");
  }

  auto member = iter->second;
  *old_info = member->Info();

  auto status = member->ClearAll();
  if (!status.ok()) {
    LOG(ERROR) << "Deregister member failed: member = "
               << member->Info().ShortDebugString()
               << ", status = " << status.ToString();
    return status;
  }

  endpoint2id_.erase(ep);
  id2member_.erase(iter);

  LOG(INFO) << "Deregister member success: endpoint = " << ep
            << ", member = " << member->Info().ShortDebugString();
  return status;
}

Status Members::DeleteMemberId(const std::string& member_id) {
  if (idset_.count(member_id) == 0) {
    return Status::NotFound("member id not found");
  } else if (id2member_.count(member_id) != 0) {
    return Status::InvalidParam("member id is still registered");
  }

  auto rc = storage_->Delete(Codec::EncodeMemberId(member_id));
  if (rc != EtcdErrCode::EtcdOK) {
    LOG(ERROR) << "Delete member id failed: member_id = " << member_id
               << ", rc = " << rc;
    return Status::Internal("delete member id failed");
  }

  idset_.erase(member_id);
  return Status::OK();
}

std::vector<MemberSPtr> Members::GetAllMembers() {
  std::vector<MemberSPtr> members;
  members.reserve(id2member_.size());
  for (const auto& item : id2member_) {
    members.emplace_back(item.second);
  }
  return members;
}

std::vector<PBCacheGroupMember> Members::GetAllInfos() {
  std::vector<PBCacheGroupMember> infos;
  infos.reserve(idset_.size());
  for (const auto& id : idset_) {
    auto iter = id2member_.find(id);
    if (iter != id2member_.end()) {
      infos.emplace_back(iter->second->Info());
    } else {
      infos.emplace_back(NewMemberInfo(id, EndPoint("", 0), 0));
    }
  }
  return infos;
}

PBCacheGroupMember Members::NewMemberInfo(const std::string& member_id,
                                          const EndPoint& endpoint,
                                          uint64_t create_time_s) {
  PBCacheGroupMember info;
  info.set_id(member_id);
  info.set_ip(endpoint.ip);
  info.set_port(endpoint.port);
  info.set_weight(0);
  info.set_create_time_s(create_time_s);
  info.set_last_online_time_ms(0);
  info.set_state(PBCacheGroupMemberState::CacheGroupMemberStateUnknown);
  return info;
}

}  // namespace cachegroup
}  // namespace mds
}  // namespace dingofs
