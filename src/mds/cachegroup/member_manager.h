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

#ifndef DINGOFS_MDS_MEMBER_MANAGER_H_
#define DINGOFS_MDS_MEMBER_MANAGER_H_

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

#include "dingofs/mds.pb.h"
#include "mds/common/context.h"
#include "mds/common/status.h"
#include "mds/common/type.h"
#include "mds/filesystem/store_operation.h"

namespace dingofs {
namespace mds {

class CacheGroupMemberManager;
using CacheGroupMemberManagerSPtr = std::shared_ptr<CacheGroupMemberManager>;

class CacheGroupMemberManager {
 public:
  CacheGroupMemberManager(OperationProcessorSPtr operation_processor) : operation_processor_(operation_processor) {};
  ~CacheGroupMemberManager() = default;

  CacheGroupMemberManager(const CacheGroupMemberManager&) = delete;
  CacheGroupMemberManager& operator=(const CacheGroupMemberManager&) = delete;
  CacheGroupMemberManager(CacheGroupMemberManager&&) = delete;
  CacheGroupMemberManager& operator=(CacheGroupMemberManager&&) = delete;

  static CacheGroupMemberManagerSPtr New(OperationProcessorSPtr operation_processor) {
    return std::make_shared<CacheGroupMemberManager>(operation_processor);
  }

  Status ReweightMember(Context& ctx, const std::string& member_id, const std::string& ip, uint32_t port,
                        uint32_t weight);

  Status JoinCacheGroup(Context& ctx, const std::string& group_name, const std::string& ip, uint32_t port,
                        uint32_t weight, const std::string& member_id);

  Status LeaveCacheGroup(Context& ctx, const std::string& group_name, const std::string& member_id,
                         const std::string& ip, uint32_t port);

  Status ListGroups(Context& ctx, std::unordered_set<std::string>& groups);

  Status ListMembers(Context& ctx, const std::string& group_name, std::vector<CacheMemberEntry>& members);

  Status ListMembers(Context& ctx, std::vector<CacheMemberEntry>& members);

  Status UnlockMember(Context& ctx, const std::string& member_id, const std::string& ip, uint32_t port);

  Status DeleteMember(Context& ctx, const std::string& member_id);

  Status UpsertCacheMember(Context& ctx, const std::string& member_id,
                           std::function<Status(CacheMemberEntry&, const Status&)> handler);

  Status GetCacheMember(Context& ctx, const std::string& member_id, CacheMemberEntry& cache_member);

  Status ListCacheMemberFromStore(Context& ctx, std::vector<CacheMemberEntry>& cache_member_entries);

  bool CheckMatchMember(std::string ip, uint32_t port, CacheMemberEntry& cache_member);

  bool CheckMemberLocked(CacheMemberEntry& cache_member);

  bool CheckMemberOffLine(CacheMemberEntry& cache_member);

  void UpsertCacheMemberToCache(const CacheMemberEntry& cache_member);
  void DeleteCacheMemberFromCache(const std::string& member_id);
  bool LoadCacheMembers();

 private:
  OperationProcessorSPtr operation_processor_;
  utils::RWLock lock_;
  // member_id -> member
  std::unordered_map<std::string, CacheMemberEntry> member_cache_;
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_FS_STAT_H_