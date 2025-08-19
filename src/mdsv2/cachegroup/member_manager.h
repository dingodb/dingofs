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

#ifndef DINGOFS_MDSV2_MEMBER_MANAGER_H_
#define DINGOFS_MDSV2_MEMBER_MANAGER_H_

#include <cstdint>
#include <memory>
#include <unordered_map>

#include "dingofs/mdsv2.pb.h"
#include "mdsv2/common/context.h"
#include "mdsv2/common/status.h"
#include "mdsv2/common/type.h"
#include "mdsv2/filesystem/store_operation.h"

namespace dingofs {
namespace mdsv2 {

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

  Status UnlockMember(Context& ctx, const std::string& member_id, const std::string& ip, uint32_t port);

  Status UpsertCacheMember(Context& ctx, const std::string& member_id,
                           std::function<Status(CacheMemberEntry&, const Status&)> handler);

  Status ListCacheMemberFromStore(Context& ctx, std::vector<CacheMemberEntry>& cache_member_entries);

  bool CheckMatchMember(std::string ip, uint32_t port, CacheMemberEntry& cache_member);

  bool CheckMemberLocked(CacheMemberEntry& cache_member);

 private:
  OperationProcessorSPtr operation_processor_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDSV2_FS_STAT_H_