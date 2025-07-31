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

#ifndef DINGOFS_SRC_MDS_CACHEGROUP_CACHE_GROUP_MEMBER_MANAGER_H_
#define DINGOFS_SRC_MDS_CACHEGROUP_CACHE_GROUP_MEMBER_MANAGER_H_

#include <cstdint>
#include <string>

#include "mds/cachegroup/cache_group_member_storage.h"
#include "mds/cachegroup/common.h"
#include "mds/kvstorageclient/etcd_client.h"

namespace dingofs {
namespace mds {
namespace cachegroup {

class CacheGroupMemberManager {
 public:
  virtual ~CacheGroupMemberManager() = default;

  virtual Status Start() = 0;
  virtual Status Shutdown() = 0;

  virtual Status JoinCacheGroup(const std::string& group_name,
                                const std::string& ip, uint32_t port,
                                uint32_t weight, uint64_t replace_id,
                                uint64_t* member_id,
                                std::string* member_uuid) = 0;
  virtual Status LeaveCacheGroup(const std::string& group_name,
                                 const std::string& ip, uint32_t port) = 0;

  virtual Status Heartbeat(const std::string& ip, uint32_t port) = 0;

  virtual std::vector<std::string> GetGroupNames() = 0;
  virtual Status GetMembers(const std::string& group_name,
                            std::vector<PBCacheGroupMember>* members_out) = 0;

  virtual Status ReweightMember(uint64_t member_id, uint32_t weight) = 0;
};

using CacheGroupMemberManagerSPtr = std::shared_ptr<CacheGroupMemberManager>;

class CacheGroupMemberManagerImpl : public CacheGroupMemberManager {
 public:
  explicit CacheGroupMemberManagerImpl(kvstorage::KVStorageClientSPtr storage);

  ~CacheGroupMemberManagerImpl() override = default;

  Status Start() override;
  Status Shutdown() override;

  Status JoinCacheGroup(const std::string& group_name, const std::string& ip,
                        uint32_t port, uint32_t weight, uint64_t replace_id,
                        uint64_t* member_id, std::string* member_uuid) override;
  Status LeaveCacheGroup(const std::string& group_name, const std::string& ip,
                         uint32_t port) override;

  Status Heartbeat(const std::string& ip, uint32_t port) override;

  std::vector<std::string> GetGroupNames() override;
  Status GetMembers(const std::string& group_name,
                    std::vector<PBCacheGroupMember>* members_out) override;

  Status ReweightMember(uint64_t member_id, uint32_t weight) override;

 private:
  Status GetOrCreateGroup(const std::string& group_name, GroupSPtr& group);
  Status GetOrCreateMember(const std::string& ip, uint32_t port,
                           MemberSPtr& member);

  std::atomic<bool> running_;
  MembersUPtr members_;
  GroupsUPtr groups_;
};

}  // namespace cachegroup
}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_SRC_MDS_CACHEGROUP_CACHE_GROUP_MEMBER_MANAGER_H_
