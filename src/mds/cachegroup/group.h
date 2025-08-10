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

#ifndef DINGOFS_SRC_MDS_CACHEGROUP_GROUP_H_
#define DINGOFS_SRC_MDS_CACHEGROUP_GROUP_H_

#include <string>
#include <unordered_map>

#include "common/status.h"
#include "mds/cachegroup/member.h"
#include "mds/kvstorageclient/etcd_client.h"

namespace dingofs {
namespace mds {
namespace cachegroup {

class Group {
 public:
  explicit Group(const std::string& group_name);

  void AddMember(const std::string& member_id, MemberSPtr member);
  void RemoveMember(const std::string& member_id);
  std::vector<MemberSPtr> GetAllMembers();

 private:
  const std::string group_name_;
  std::unordered_map<std::string, MemberSPtr> id2member_;
};

using GroupSPtr = std::shared_ptr<Group>;

class Groups {
 public:
  explicit Groups(kvstorage::KVStorageClientSPtr storage);

  Status Load();

  Status GetGroup(const std::string& group_name, GroupSPtr& group);
  Status RegisterGroup(const std::string& group_name, GroupSPtr& group);
  std::vector<std::string> GetAllGroupNames();

 private:
  Status CheckGroupName(const std::string& group_name);
  Status StoreGroupName(const std::string& group_name);

  kvstorage::KVStorageClientSPtr storage_;
  std::unordered_map<std::string, GroupSPtr> name2group_;
};

using GroupsUPtr = std::unique_ptr<Groups>;

}  // namespace cachegroup
}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_SRC_MDS_CACHEGROUP_GROUP_H_
