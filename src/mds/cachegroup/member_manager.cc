// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "mds/cachegroup/member_manager.h"

#include <cstdint>
#include <string>
#include <utility>

#include "common/options/mds/option.h"
#include "gflags/gflags.h"
#include "mds/common/codec.h"
#include "mds/common/context.h"
#include "mds/common/helper.h"
#include "mds/common/logging.h"
#include "mds/common/runnable.h"
#include "mds/common/status.h"
#include "mds/common/type.h"
#include "mds/filesystem/store_operation.h"
#include "mds/storage/storage.h"

namespace dingofs {
namespace mds {

DEFINE_uint32(cache_member_heartbeat_miss_timeout_s, 30, "Timeout for missing heartbeat in seconds");

DEFINE_uint32(cache_member_heartbeat_offline_timeout_s, 60, "Timeout for member to be considered offline in seconds");

Status CacheGroupMemberManager::ReweightMember(Context& ctx, const std::string& member_id, const std::string& ip,
                                               uint32_t port, uint32_t weight) {
  auto handler = [this, ip, port, weight](CacheMemberEntry& cache_member, const Status& status) -> Status {
    if (!status.ok()) {
      return status;
    }
    if (!CheckMatchMember(ip, port, cache_member)) {
      return Status(pb::error::Errno::ENOT_MATCH, "cache member not match");
    }
    cache_member.set_weight(weight);
    return Status::OK();
  };
  return UpsertCacheMember(ctx, member_id, handler);
}

static void SetMemberState(CacheMemberEntry& cache_member) {
  auto now_ms = Helper::TimestampMs();
  if (cache_member.last_online_time_ms() == 0) {
    cache_member.set_state(pb::mds::CacheGroupMemberState::CacheGroupMemberStateUnknown);
  } else if (cache_member.last_online_time_ms() + FLAGS_cache_member_heartbeat_offline_timeout_s * 1000 <
             static_cast<uint64_t>(now_ms)) {
    cache_member.set_state(pb::mds::CacheGroupMemberState::CacheGroupMemberStateOffline);
  } else if (cache_member.last_online_time_ms() + FLAGS_cache_member_heartbeat_miss_timeout_s * 1000 <
             static_cast<uint64_t>(now_ms)) {
    cache_member.set_state(pb::mds::CacheGroupMemberState::CacheGroupMemberStateUnstable);
  } else {
    cache_member.set_state(pb::mds::CacheGroupMemberState::CacheGroupMemberStateOnline);
  }
  return;
}

Status CacheGroupMemberManager::ListMembers(Context& ctx, std::vector<CacheMemberEntry>& members) {
  if (FLAGS_mds_cache_member_enable_cache) {
    utils::ReadLockGuard lock(lock_);

    members.reserve(member_cache_.size());
    for (const auto& pair : member_cache_) {
      members.push_back(pair.second);
    }

    if (members.size() > 0) {
      return Status::OK();
    }
  }

  return ListCacheMemberFromStore(ctx, members);
}

Status CacheGroupMemberManager::ListMembers(Context& ctx, const std::string& group_name,
                                            std::vector<CacheMemberEntry>& members) {
  std::vector<CacheMemberEntry> cache_member_entries;
  auto status = ListMembers(ctx, cache_member_entries);
  if (!status.ok()) {
    return status;
  }

  if (group_name.empty()) {
    for (auto& cache_member : cache_member_entries) {
      SetMemberState(cache_member);
      members.emplace_back(cache_member);
    }
  } else {
    for (auto& cache_member : cache_member_entries) {
      if (cache_member.group_name() != group_name) {
        continue;
      }
      SetMemberState(cache_member);
      members.emplace_back(cache_member);
    }
  }

  return Status::OK();
}

Status CacheGroupMemberManager::JoinCacheGroup(Context& ctx, const std::string& group_name, const std::string& ip,
                                               uint32_t port, uint32_t weight, const std::string& member_id) {
  auto handler = [this, member_id, ip, port, group_name, weight](CacheMemberEntry& cache_member,
                                                                 const Status& status) -> Status {
    if (!status.ok()) {
      if (status.error_code() != pb::error::ENOT_FOUND) {
        return status;
      }

      cache_member.set_member_id(member_id);
      cache_member.set_ip(ip);
      cache_member.set_port(port);
      cache_member.set_weight(weight);
      cache_member.set_create_time_s(Helper::Timestamp());
      cache_member.set_last_online_time_ms(0);
      cache_member.set_group_name(group_name);
      cache_member.set_state(pb::mds::CacheGroupMemberState::CacheGroupMemberStateUnknown);
      cache_member.set_locked(true);
    } else {
      if (CheckMatchMember(ip, port, cache_member)) {
        cache_member.set_weight(weight);
        cache_member.set_group_name(group_name);
        cache_member.set_locked(true);
      } else {
        // not match and locked
        if (CheckMemberLocked(cache_member)) {
          return Status(pb::error::Errno::ELOCKED, "cache member has locked");
        }
        // update cache member info
        cache_member.set_ip(ip);
        cache_member.set_port(port);
        cache_member.set_weight(weight);
        cache_member.set_group_name(group_name);
        cache_member.set_locked(true);
      }
    }
    return Status::OK();
  };

  return UpsertCacheMember(ctx, member_id, handler);
}

Status CacheGroupMemberManager::LeaveCacheGroup(Context& ctx, const std::string& group_name,
                                                const std::string& member_id, const std::string& ip, uint32_t port) {
  auto handler = [this, ip, port, group_name](CacheMemberEntry& cache_member, const Status& status) -> Status {
    if (!status.ok()) {
      return status;
    }

    if (!CheckMatchMember(ip, port, cache_member)) {
      return Status(pb::error::Errno::ENOT_MATCH, "cache member not match");
    }

    if (group_name != cache_member.group_name()) {
      return Status(pb::error::Errno::ENOT_MATCH, "group not match");
    }

    cache_member.clear_group_name();
    return Status::OK();
  };

  return UpsertCacheMember(ctx, member_id, handler);
}

Status CacheGroupMemberManager::ListGroups(Context& ctx, std::unordered_set<std::string>& groups) {
  std::vector<CacheMemberEntry> cache_member_entries;
  auto status = ListMembers(ctx, cache_member_entries);
  if (!status.ok()) {
    return status;
  }

  for (auto const& cache_member : cache_member_entries) {
    if (cache_member.group_name().empty()) {
      continue;
    }
    groups.insert(cache_member.group_name());
  }

  return Status::OK();
}

Status CacheGroupMemberManager::UnlockMember(Context& ctx, const std::string& member_id, const std::string& ip,
                                             uint32_t port) {
  auto handler = [this, ip, port](CacheMemberEntry& cache_member, const Status& status) -> Status {
    if (!status.ok()) {
      return status;
    }

    if (!CheckMatchMember(ip, port, cache_member)) {
      return Status(pb::error::Errno::ENOT_MATCH, "cache member not match");
    }

    if (!CheckMemberOffLine(cache_member)) {
      return Status(pb::error::Errno::EINVALID_STATE, "cache member state not offline");
    }

    cache_member.set_locked(false);
    return Status::OK();
  };

  return UpsertCacheMember(ctx, member_id, handler);
}

Status CacheGroupMemberManager::GetCacheMember(Context& ctx, const std::string& member_id,
                                               CacheMemberEntry& cache_member) {
  if (FLAGS_mds_cache_member_enable_cache) {
    utils::ReadLockGuard lock(lock_);

    auto it = member_cache_.find(member_id);
    if (it != member_cache_.end()) {
      cache_member = it->second;
      return Status::OK();
    }
  }

  auto& trace = ctx.GetTrace();
  GetCacheMemberOperation operation(trace, member_id);
  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[cachegroup] get cache member fail, error({}).", status.error_str());
    return status;
  }

  auto& result = operation.GetResult();
  cache_member = std::move(result.cache_member);

  UpsertCacheMemberToCache(cache_member);
  return Status::OK();
}

Status CacheGroupMemberManager::DeleteMember(Context& ctx, const std::string& member_id) {
  CacheMemberEntry cache_member;
  auto status = GetCacheMember(ctx, member_id, cache_member);
  if (!status.ok()) {
    return status;
  }

  if (!CheckMemberOffLine(cache_member)) {
    return Status(pb::error::Errno::EINVALID_STATE, "cache member state not offline");
  }

  auto& trace = ctx.GetTrace();
  DeleteCacheMemberOperation operation(trace, member_id);

  status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[cachegroup] delete cache member fail, error({}).", status.error_str());
    return status;
  }

  DeleteCacheMemberFromCache(member_id);

  return Status::OK();
}

Status CacheGroupMemberManager::UpsertCacheMember(Context& ctx, const std::string& member_id,
                                                  std::function<Status(CacheMemberEntry&, const Status&)> handler) {
  auto& trace = ctx.GetTrace();
  UpsertCacheMemberOperation operation(trace, member_id, handler);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[cachegroup] upsert cache member fail, error({}).", status.error_str());
    return status;
  }

  auto& result = operation.GetResult();
  UpsertCacheMemberToCache(result.cache_member);
  return Status::OK();
}

Status CacheGroupMemberManager::ListCacheMemberFromStore(Context& ctx,
                                                         std::vector<CacheMemberEntry>& cache_member_entries) {
  auto& trace = ctx.GetTrace();
  ScanCacheMemberOperation operation(trace);
  operation.SetIsolationLevel(Txn::kReadCommitted);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[cachegroup] list cache member fail, error({}).", status.error_str());
    return status;
  }

  auto& result = operation.GetResult();
  cache_member_entries = std::move(result.cache_member_entries);
  return Status::OK();
}

bool CacheGroupMemberManager::CheckMatchMember(std::string ip, uint32_t port, CacheMemberEntry& cache_member) {
  return ip == cache_member.ip() && port == cache_member.port() ? true : false;
}

bool CacheGroupMemberManager::CheckMemberLocked(CacheMemberEntry& cache_member) { return cache_member.locked(); }

bool CacheGroupMemberManager::CheckMemberOffLine(CacheMemberEntry& cache_member) {
  return cache_member.last_online_time_ms() + FLAGS_cache_member_heartbeat_offline_timeout_s * 1000 <
                 static_cast<uint64_t>(Helper::TimestampMs())
             ? true
             : false;
}

void CacheGroupMemberManager::UpsertCacheMemberToCache(const CacheMemberEntry& cache_member) {
  if (!FLAGS_mds_cache_member_enable_cache) {
    return;
  }
  utils::WriteLockGuard lock(lock_);

  auto it = member_cache_.find(cache_member.member_id());
  if (it != member_cache_.end()) {
    if (it->second.version() >= cache_member.version()) {
      return;
    }
  }
  member_cache_.emplace(cache_member.member_id(), std::move(cache_member));
}

void CacheGroupMemberManager::DeleteCacheMemberFromCache(const std::string& member_id) {
  if (!FLAGS_mds_cache_member_enable_cache) {
    return;
  }

  utils::WriteLockGuard lock(lock_);
  member_cache_.erase(member_id);
}

bool CacheGroupMemberManager::LoadCacheMembers() {
  if (!FLAGS_mds_cache_member_enable_cache) {
    return true;
  }

  std::vector<CacheMemberEntry> cache_member_entries;
  Context ctx;
  auto status = ListCacheMemberFromStore(ctx, cache_member_entries);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[cachegroup] load cache members fail, error({}).", status.error_str());
    return false;
  }

  utils::WriteLockGuard lock(lock_);

  member_cache_.clear();
  for (auto& member : cache_member_entries) {
    member_cache_.emplace(member.member_id(), std::move(member));
  }

  return true;
}

}  // namespace mds
}  // namespace dingofs