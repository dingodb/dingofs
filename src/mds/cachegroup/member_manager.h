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

#ifndef DINGOFS_SRC_MDS_CACHEGROUP_MEMBER_MANAGER_H_
#define DINGOFS_SRC_MDS_CACHEGROUP_MEMBER_MANAGER_H_

#include <cstdint>
#include <string>

#include "mds/cachegroup/common.h"
#include "mds/cachegroup/group.h"
#include "mds/cachegroup/member.h"
#include "mds/kvstorageclient/etcd_client.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace mds {
namespace cachegroup {

class CacheGroupMemberManager {
 public:
  virtual ~CacheGroupMemberManager() = default;

  virtual Status Start() = 0;
  virtual Status Shutdown() = 0;

  // member
  virtual Status RegisterMember(const EndPoint& endpoint,
                                const std::string& want_id,
                                std::string* member_id) = 0;
  virtual Status DeregisterMember(const EndPoint& endpoint) = 0;
  virtual Status DeleteMemberId(const std::string& member_id) = 0;
  virtual Status MemberHeartbeat(const EndPoint& endpoint) = 0;
  virtual Status ReweightMember(const std::string& member_id,
                                uint32_t weight) = 0;
  virtual std::vector<PBCacheGroupMember> ListAllMembers() = 0;

  // group
  virtual Status JoinCacheGroup(const std::string& group_name,
                                const EndPoint& endpoint, uint32_t weight,
                                std::string* member_id) = 0;
  virtual Status LeaveCacheGroup(const std::string& group_name,
                                 const EndPoint& endpoint) = 0;
  virtual Status LeaveCacheGroup(const std::string& group_name,
                                 const std::string& member_id) = 0;
  virtual std::vector<std::string> ListGroups() = 0;
  virtual Status ListGroupMembers(const std::string& group_name,
                                  std::vector<PBCacheGroupMember>* members) = 0;
};

using CacheGroupMemberManagerSPtr = std::shared_ptr<CacheGroupMemberManager>;

class CacheGroupMemberManagerImpl : public CacheGroupMemberManager {
 public:
  explicit CacheGroupMemberManagerImpl(kvstorage::KVStorageClientSPtr storage);

  ~CacheGroupMemberManagerImpl() override = default;

  Status Start() override;
  Status Shutdown() override;

  Status RegisterMember(const EndPoint& endpoint, const std::string& want_id,
                        std::string* member_id) override;
  Status DeregisterMember(const EndPoint& endpoint) override;
  Status DeleteMemberId(const std::string& member_id) override;
  Status MemberHeartbeat(const EndPoint& endpoint) override;
  Status ReweightMember(const std::string& member_id, uint32_t weight) override;
  std::vector<PBCacheGroupMember> ListAllMembers() override;

  Status JoinCacheGroup(const std::string& group_name, const EndPoint& endpoint,
                        uint32_t weight, std::string* member_id) override;
  Status LeaveCacheGroup(const std::string& group_name,
                         const EndPoint& endpoint) override;
  Status LeaveCacheGroup(const std::string& group_name,
                         const std::string& member_id) override;
  std::vector<std::string> ListGroups() override;
  Status ListGroupMembers(const std::string& group_name,
                          std::vector<PBCacheGroupMember>* members) override;

 private:
  Status GetOrRegisterGroup(const std::string& group_name, GroupSPtr& group);
  Status GetOrRegisterMember(const EndPoint& endpoint, MemberSPtr& member);
  Status LeaveGroup(const std::string& group_name, MemberSPtr member);
  void LeaveGroup(const std::string& group_name, const std::string& member_id);

  // TODO: more efficient lock!!
  // The deregister leads complexity without transaction, but it not bad now,
  // because only heartbeat and list are frequent.
  std::atomic<bool> running_;
  utils::RWLock rwlock_;
  MembersUPtr members_;
  GroupsUPtr groups_;
};

}  // namespace cachegroup
}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_SRC_MDS_CACHEGROUP_MEMBER_MANAGER_H_
