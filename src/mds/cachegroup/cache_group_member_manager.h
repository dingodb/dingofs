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
#include <memory>
#include <string>

#include "dingofs/cachegroup.pb.h"
#include "mds/cachegroup/cache_group_member_storage.h"
#include "mds/cachegroup/config.h"
#include "mds/cachegroup/errno.h"

namespace dingofs {
namespace mds {
namespace cachegroup {

using ::dingofs::pb::mds::cachegroup::CacheGroupMember;
using ::dingofs::utils::BthreadRWLock;
using Statistic = ::dingofs::pb::mds::cachegroup::HeartbeatRequest::Statistic;

class CacheGroupMemberManager {
 public:
  virtual ~CacheGroupMemberManager() = default;

  virtual bool Init() = 0;

  virtual Errno RegisterMember(uint64_t old_id, uint64_t* member_id) = 0;

  virtual Errno AddMember(const std::string& group_name,
                          CacheGroupMember member) = 0;

  virtual Errno LoadMembers(const std::string& group_name,
                            std::vector<CacheGroupMember>* members_out) = 0;

  virtual Errno ReweightMember(const std::string& group_name,
                               uint64_t member_id, uint32_t weight) = 0;

  virtual Errno HandleHeartbeat(const std::string& group_name,
                                uint64_t member_id, const Statistic& stat) = 0;

  virtual std::vector<std::string> LoadGroups() = 0;
};

class CacheGroupMemberManagerImpl : public CacheGroupMemberManager {
 public:
  CacheGroupMemberManagerImpl(CacheGroupOption option,
                              std::shared_ptr<KVStorageClient> kv);

  ~CacheGroupMemberManagerImpl() override = default;

  bool Init() override;

  Errno RegisterMember(uint64_t old_id, uint64_t* member_id) override;

  Errno AddMember(const std::string& group_name,
                  CacheGroupMember member) override;

  Errno LoadMembers(const std::string& group_name,
                    std::vector<CacheGroupMember>* members_out) override;

  Errno ReweightMember(const std::string& group_name, uint64_t member_id,
                       uint32_t weight) override;

  Errno HandleHeartbeat(const std::string& group_name, uint64_t member_id,
                        const Statistic& stat) override;

  std::vector<std::string> LoadGroups() override;

 private:
  void SetMembersState(std::vector<CacheGroupMember>* members);
  void FilterOutOfflineMember(const std::vector<CacheGroupMember>& members,
                              std::vector<CacheGroupMember>* members_out);

  bool CheckId(uint64_t member_id);

  uint64_t TimestampMs();

 private:
  CacheGroupOption option_;
  std::unique_ptr<CacheGroupMemberStorage> storage_;
};

}  // namespace cachegroup
}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_SRC_MDS_CACHEGROUP_CACHE_GROUP_MEMBER_MANAGER_H_
