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

#include "mdsv2/cachegroup/member_manager.h"

#include <cstdint>
#include <string>
#include <utility>

#include "gflags/gflags.h"
#include "mdsv2/common/codec.h"
#include "mdsv2/common/context.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/common/runnable.h"
#include "mdsv2/common/status.h"
#include "mdsv2/common/type.h"
#include "mdsv2/filesystem/store_operation.h"
#include "mdsv2/storage/storage.h"

namespace dingofs {
namespace mdsv2 {

DEFINE_uint32(cache_member_heartbeat_miss_timeout_s, 10, "Timeout for missing heartbeat in seconds");

DEFINE_uint32(cache_member_heartbeat_offline_timeout_s, 30, "Timeout for member to be considered offline in seconds");

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
    cache_member.set_state(pb::mdsv2::CacheGroupMemberState::CacheGroupMemberStateUnknown);
  } else if (cache_member.last_online_time_ms() + FLAGS_cache_member_heartbeat_offline_timeout_s * 1000 <
             static_cast<uint64_t>(now_ms)) {
    cache_member.set_state(pb::mdsv2::CacheGroupMemberState::CacheGroupMemberStateOffline);
  } else if (cache_member.last_online_time_ms() + FLAGS_cache_member_heartbeat_miss_timeout_s * 1000 <
             static_cast<uint64_t>(now_ms)) {
    cache_member.set_state(pb::mdsv2::CacheGroupMemberState::CacheGroupMemberStateUnstable);
  } else {
    cache_member.set_state(pb::mdsv2::CacheGroupMemberState::CacheGroupMemberStateOnline);
  }
  return;
}

Status CacheGroupMemberManager::ListMembers(Context& ctx, const std::string& group_name,
                                            std::vector<CacheMemberEntry>& members) {
  std::vector<CacheMemberEntry> cache_member_entries;
  auto status = ListCacheMemberFromStore(ctx, cache_member_entries);
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
      cache_member.set_state(pb::mdsv2::CacheGroupMemberState::CacheGroupMemberStateUnknown);
      cache_member.set_locked(true);
    } else {
      if (CheckMatchMember(ip, port, cache_member)) {
        cache_member.set_weight(weight);
        cache_member.set_group_name(group_name);
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
  auto status = ListCacheMemberFromStore(ctx, cache_member_entries);
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

    cache_member.set_locked(false);
    return Status::OK();
  };

  return UpsertCacheMember(ctx, member_id, handler);
}

Status CacheGroupMemberManager::DeleteMember(Context& ctx, const std::string& member_id) {
  auto& trace = ctx.GetTrace();
  DeleteCacheMemberOperation operation(trace, member_id);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[cachegroup] delete cache member fail, error({}).", status.error_str());
    return status;
  }

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

  return Status::OK();
}

Status CacheGroupMemberManager::ListCacheMemberFromStore(Context& ctx,
                                                         std::vector<CacheMemberEntry>& cache_member_entries) {
  auto& trace = ctx.GetTrace();
  ScanCacheMemberOperation operation(trace);

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

}  // namespace mdsv2
}  // namespace dingofs