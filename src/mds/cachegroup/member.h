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

#ifndef DINGOFS_SRC_MDS_CACHEGROUP_MEMBER_H_
#define DINGOFS_SRC_MDS_CACHEGROUP_MEMBER_H_

#include <sys/types.h>

#include <memory>
#include <string>
#include <unordered_map>

#include "common/status.h"
#include "mds/cachegroup/common.h"
#include "mds/kvstorageclient/etcd_client.h"
#include "utils/concurrent/rw_lock.h"

namespace dingofs {
namespace mds {
namespace cachegroup {

struct EndPoint {
  EndPoint(const std::string& ip, uint32_t port) : ip(ip), port(port) {}
  std::string ToString() const { return absl::StrFormat("%s:%u", ip, port); }

  const std::string ip;
  const uint32_t port;
};

class Member {
 public:
  Member(const PBCacheGroupMember& info,
         kvstorage::KVStorageClientSPtr storage);

  Status SetGroup(const std::string& group_name, uint32_t weight);
  Status SetLastOnlineTime(uint64_t last_online_time_ms);
  Status SetWeight(uint32_t weight);
  Status ClearGroup();
  Status ClearAll();
  Status Store();

  PBCacheGroupMember Info();
  bool IsDeregisterd() const;

 private:
  using UpdateFn = std::function<Status(PBCacheGroupMember* info)>;

  Status DoStore(UpdateFn fn);
  Status Store(const PBCacheGroupMember& info);
  PBCacheGroupMemberState GetState() const;

  utils::BthreadRWLock rwlock_;
  PBCacheGroupMember info_;
  kvstorage::KVStorageClientSPtr storage_;
};

using MemberSPtr = std::shared_ptr<Member>;

class Members {
 public:
  explicit Members(kvstorage::KVStorageClientSPtr storage);

  Status Load();

  Status GetMember(const EndPoint& endpoint, MemberSPtr& member);
  Status GetMember(const std::string& member_id, MemberSPtr& member);
  Status RegisterMember(const EndPoint& endpoint, MemberSPtr& member);
  Status RegisterMember(const EndPoint& endpoint, const std::string& want_id,
                        MemberSPtr& member);
  Status DeregisterMember(const EndPoint& endpoint,
                          PBCacheGroupMember* old_info);
  Status DeleteMemberId(const std::string& member_id);
  std::vector<MemberSPtr> GetAllMembers();
  std::vector<PBCacheGroupMember> GetAllInfos();  // include deregistered info

 private:
  PBCacheGroupMember NewMemberInfo(const std::string& member_id,
                                   const EndPoint& endpoint,
                                   uint64_t create_time_s);

  kvstorage::KVStorageClientSPtr storage_;
  std::unordered_set<std::string> idset_;
  std::unordered_map<std::string, std::string> endpoint2id_;
  std::unordered_map<std::string, MemberSPtr> id2member_;
};

using MembersUPtr = std::unique_ptr<Members>;

}  // namespace cachegroup
}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_SRC_MDS_CACHEGROUP_MEMBER_H_
