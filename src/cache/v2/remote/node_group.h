/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_CACHE_V2_REMOTE_NODE_GROUP_H_
#define DINGOFS_CACHE_V2_REMOTE_NODE_GROUP_H_

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "cache/v2/remote/members.h"
#include "cache/v2/remote/node.h"

namespace dingofs {
namespace cache {
namespace v2 {

class RemoteNodeGroup {
 public:
  RemoteNodeGroup() = default;
  ~RemoteNodeGroup() = default;

  RemoteNodeGroup(const RemoteNodeGroup&) = delete;
  RemoteNodeGroup& operator=(const RemoteNodeGroup&) = delete;

  Future<> Start();
  Future<> Shutdown();

  StatusOr<RemoteNode*> GetNode(uint64_t key);
  void Rebuild(MemberGroupSPtr member_group);

  const Members& members() const { return member_group_->members(); }
  bool empty() const { return member_group_->empty(); }

 private:
  void StartNodes(const std::vector<RemoteNode*>& nodes);
  void ShutdownNodes(std::vector<RemoteNodeUPtr> nodes);
  Future<> StartNode(RemoteNode* node);
  Future<> ShutdownNode(RemoteNodeUPtr node);
  RemoteNodeUPtr TakeNodeOf(const CacheGroupMember& member);
  bool IsSameEndpoint(const CacheGroupMember& a, const CacheGroupMember& b);

  bool running_ = false;
  MemberGroupSPtr member_group_{std::make_shared<const MemberGroup>(Members{})};
  std::unordered_map<std::string, RemoteNodeUPtr> nodes_;
  Gate inflight_;
};

using RemoteNodeGroupUPtr = std::unique_ptr<RemoteNodeGroup>;

inline thread_local RemoteNodeGroup* tls_node_group = nullptr;

inline RemoteNodeGroup* ThisNodeGroup() { return tls_node_group; }

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_REMOTE_NODE_GROUP_H_
