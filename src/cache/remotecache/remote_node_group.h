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
 * Created Date: 2025-06-05
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_NODE_GROUP_H_
#define DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_NODE_GROUP_H_

#include "base/hash/con_hash.h"
#include "cache/common/common.h"
#include "cache/remotecache/remote_node.h"

namespace dingofs {
namespace cache {

class RemoteNodeGroup {
 public:
  explicit RemoteNodeGroup(const PB_CacheGroupMembers& members);

  Status Init();

  RemoteNodeSPtr GetNode(const std::string& key);

  bool IsDiff(const PB_CacheGroupMembers& members) const;

 private:
  std::vector<uint64_t> CalcWeights(const PB_CacheGroupMembers& members);

  std::string MemberKey(const PB_CacheGroupMember& member) const;

  RemoteAccessOption option_;
  PB_CacheGroupMembers members_;
  std::shared_ptr<base::hash::ConHash> chash_;
  std::unordered_map<std::string, RemoteNodeSPtr> nodes_;
};

using RemoteNodeGorupSPtr = std::shared_ptr<RemoteNodeGroup>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_NODE_GROUP_H_
