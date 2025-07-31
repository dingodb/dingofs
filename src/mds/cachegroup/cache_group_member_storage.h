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

#ifndef DINGOFS_DINGOFS_SRC_MDS_CACHEGROUP_CACHE_GROUP_MEMBER_STORAGE_H_
#define DINGOFS_DINGOFS_SRC_MDS_CACHEGROUP_CACHE_GROUP_MEMBER_STORAGE_H_

#include <memory>
#include <string>
#include <unordered_map>

#include "common/status.h"
#include "mds/cachegroup/common.h"
#include "mds/idgenerator/etcd_id_generator.h"
#include "mds/kvstorageclient/etcd_client.h"
#include "utils/concurrent/rw_lock.h"

namespace dingofs {
namespace mds {
namespace cachegroup {

class Group;
using GroupSPtr = std::shared_ptr<Group>;

class Member : public std::enable_shared_from_this<Member> {
 public:
  Member(const PBCacheGroupMember& member_info,
         kvstorage::KVStorageClientSPtr storage);

  Status JoinCacheGroup(GroupSPtr group, uint32_t weight);
  Status LeaveCacheGroup();
  Status Heartbeat(uint64_t last_online_time_ms);
  Status Reweight(uint32_t weight);

  Status Store();
  Status Destroy();
  PBCacheGroupMember GetInfo();

 private:
  PBCacheGroupMemberState GetState() const;

  utils::BthreadRWLock rwlock_;
  GroupSPtr group_;
  PBCacheGroupMember member_info_;
  kvstorage::KVStorageClientSPtr storage_;
};

using MemberSPtr = std::shared_ptr<Member>;

class Members {
 public:
  explicit Members(kvstorage::KVStorageClientSPtr storage);

  Status Load();

  Status GetMember(const std::string& ip, uint32_t port, MemberSPtr& member);
  Status GetMember(uint64_t member_id, MemberSPtr& member);
  Status CreateMember(const std::string& ip, uint32_t port, MemberSPtr& member);
  Status ReplaceMember(uint64_t member_id, const std::string& ip, uint32_t port,
                       MemberSPtr& member);

  std::vector<MemberSPtr> GetAllMembers();

 private:
  PBCacheGroupMember NewMemberInfo(uint64_t member_id, const std::string& ip,
                                   uint32_t port);

  utils::BthreadRWLock rwlock_;
  kvstorage::KVStorageClientSPtr storage_;
  idgenerator::IdAllocatorUPtr member_id_generator_;
  std::unordered_map<std::string, uint64_t> endpoint2id_;
  std::unordered_map<uint64_t, MemberSPtr> id2member_;
};

using MembersUPtr = std::unique_ptr<Members>;

class Group {
 public:
  explicit Group(const std::string& group_name);

  void AddMember(MemberSPtr member);
  void RemoveMember(uint64_t member_id);
  std::vector<MemberSPtr> GetAllMembers();

  size_t Size();
  std::string Name() const;

 private:
  utils::BthreadRWLock rwlock_;
  const std::string group_name_;
  std::unordered_map<uint64_t, MemberSPtr> id2member_;
};

class Groups {
 public:
  explicit Groups(kvstorage::KVStorageClientSPtr storage);

  Status Load();

  Status GetGroup(const std::string& group_name, GroupSPtr& group);
  Status CreateGroup(const std::string& group_name, GroupSPtr& group);
  std::vector<std::string> GetAllGroupNames();

 private:
  Status CheckGroupName(const std::string& group_name);
  Status StoreGroupName(const std::string& group_name);

  utils::BthreadRWLock rwlock_;
  kvstorage::KVStorageClientSPtr storage_;
  std::unordered_map<std::string, GroupSPtr> name2group_;
};

using GroupsUPtr = std::unique_ptr<Groups>;

}  // namespace cachegroup
}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_DINGOFS_SRC_MDS_CACHEGROUP_CACHE_GROUP_MEMBER_STORAGE_H_
