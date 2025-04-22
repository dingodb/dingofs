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

#ifndef DINGOFS_SRC_MDS_CACHEGROUP_CACHE_GROUP_MEMBER_STORAGE_H_
#define DINGOFS_SRC_MDS_CACHEGROUP_CACHE_GROUP_MEMBER_STORAGE_H_

#include <memory>
#include <string>
#include <unordered_map>

#include "dingofs/cachegroup.pb.h"
#include "mds/cachegroup/errno.h"
#include "mds/idgenerator/etcd_id_generator.h"
#include "mds/kvstorageclient/etcd_client.h"
#include "utils/concurrent/concurrent.h"
#include "utils/concurrent/rw_lock.h"

namespace dingofs {
namespace mds {
namespace cachegroup {

using ::dingofs::idgenerator::EtcdIdGenerator;
using ::dingofs::kvstorage::KVStorageClient;
using ::dingofs::pb::mds::cachegroup::CacheGroupMember;
using ::dingofs::utils::BthreadRWLock;

class CacheGroupMemberStorage {
 public:
  virtual ~CacheGroupMemberStorage() = default;

  virtual bool Init() = 0;

  virtual Errno GetGroupId(const std::string& group_name, uint64_t* id) = 0;

  virtual Errno RegisterMember(uint64_t* id) = 0;

  virtual Errno RegisterGroup(const std::string& group_name, uint64_t* id) = 0;

  virtual Errno AddMember(uint64_t group_id,
                          const CacheGroupMember& member) = 0;

  virtual void LoadMembers(uint64_t group_id,
                           std::vector<CacheGroupMember>* members) = 0;

  virtual Errno ReweightMember(uint64_t group_id, uint64_t member_id,
                               uint32_t weight) = 0;
};

class CacheGroupMemberStorageImpl : public CacheGroupMemberStorage {
  using CacheGroupMembersType = std::unordered_map<uint64_t, CacheGroupMember>;

 public:
  explicit CacheGroupMemberStorageImpl(std::shared_ptr<KVStorageClient> kv);

  bool Init() override;

  Errno GetGroupId(const std::string& group_name, uint64_t* group_id) override;

  Errno RegisterMember(uint64_t* member_id) override;

  Errno RegisterGroup(const std::string& group_name,
                      uint64_t* group_id) override;

  Errno AddMember(uint64_t group_id, const CacheGroupMember& member) override;

  void LoadMembers(uint64_t group_id,
                   std::vector<CacheGroupMember>* members) override;

  Errno ReweightMember(uint64_t group_id, uint64_t member_id,
                       uint32_t weight) override;

 private:
  bool LoadGroupNames();

  bool LoadGroupMembers();

  void AddMember2Group(uint64_t group_id, uint64_t member_id,
                       const CacheGroupMember& member);

  bool StoreGroupName(uint64_t group_id, const std::string& group_name);

  bool StoreGroupMember(uint64_t group_id, uint64_t member_id,
                        const CacheGroupMember& member);

 private:
  BthreadRWLock rwlock_;  // TODO(Wine93): more efficient for multi-locks
  std::shared_ptr<KVStorageClient> kv_;
  // member id (start with 1)
  std::unique_ptr<EtcdIdGenerator> member_id_generator_;
  // group id (start with 1)
  std::unique_ptr<EtcdIdGenerator> group_id_generator_;
  // group_name => group_id
  std::unordered_map<std::string, uint64_t> group_names_;
  // group_id => cache_group_members
  std::unordered_map<uint64_t, CacheGroupMembersType> groups_;
};

}  // namespace cachegroup
}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_SRC_MDS_CACHEGROUP_CACHE_GROUP_MEMBER_STORAGE_H_
