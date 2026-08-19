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

#include "blockcache/remote/node_group.h"

#include <glog/logging.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "blockcache/core/runtime/smp.h"

namespace dingofs {
namespace blockcache {

Future<> RemoteNodeGroup::Start() {
  LOG(INFO) << "RemoteNodeGroup{shard=" << ThisShardId() << "} is starting...";

  running_ = true;
  tls_node_group = this;

  LOG(INFO) << "Successfully start RemoteNodeGroup{shard=" << ThisShardId()
            << "}";
  co_return;
}

Future<> RemoteNodeGroup::Shutdown() {
  if (!running_) {
    co_return;
  }

  running_ = false;
  tls_node_group = nullptr;

  LOG(INFO) << "RemoteNodeGroup{shard=" << ThisShardId()
            << "} is shutting down...";

  member_group_ = std::make_shared<const MemberGroup>(Members{});

  std::unordered_map<std::string, RemoteNodeUPtr> nodes = std::move(nodes_);
  nodes_.clear();
  for (auto& [id, node] : nodes) {
    co_await node->Shutdown();
  }

  co_await inflight_.Close();

  LOG(INFO) << "Successfully shutdown RemoteNodeGroup{shard=" << ThisShardId()
            << "}";
}

StatusOr<RemoteNode*> RemoteNodeGroup::GetNode(uint64_t key) {
  if (!running_) {
    return Status::CacheDown("RemoteNodeGroup is down");
  }

  const CacheGroupMember* member = member_group_->GetMember(key);
  if (member == nullptr) {
    return Status::NotFound("MemberGroup is empty");
  }

  const auto iter = nodes_.find(member->id);
  if (iter == nodes_.end()) {
    return Status::NotFound("RemoteNode{id=" + member->id + "} not found");
  }

  return iter->second.get();
}

void RemoteNodeGroup::Rebuild(MemberGroupSPtr member_group) {
  if (member_group == member_group_) {
    return;
  }

  std::unordered_map<std::string, RemoteNodeUPtr> nodes;
  std::vector<RemoteNode*> to_start;
  nodes.reserve(member_group->size());

  for (const CacheGroupMember& member : member_group->members()) {
    RemoteNodeUPtr node = TakeNodeOf(member);
    if (node == nullptr) {
      node = std::make_unique<RemoteNode>(member);
      to_start.push_back(node.get());
    }
    nodes.emplace(member.id, std::move(node));
  }

  std::vector<RemoteNodeUPtr> to_shutdown;
  for (auto& [id, node] : nodes_) {
    if (node != nullptr) {
      to_shutdown.push_back(std::move(node));
    }
  }

  StartNodes(to_start);
  member_group_ = std::move(member_group);
  nodes_ = std::move(nodes);
  ShutdownNodes(std::move(to_shutdown));
}

void RemoteNodeGroup::StartNodes(const std::vector<RemoteNode*>& nodes) {
  for (RemoteNode* node : nodes) {
    (void)StartNode(node);
  }
}

void RemoteNodeGroup::ShutdownNodes(std::vector<RemoteNodeUPtr> nodes) {
  for (RemoteNodeUPtr& node : nodes) {
    (void)ShutdownNode(std::move(node));
  }
}

Future<> RemoteNodeGroup::StartNode(RemoteNode* node) {
  Gate::Holder holder(inflight_);
  if (!holder.ok()) {
    co_return;
  }

  const std::string id = node->member().id;
  const Status status = co_await node->Start();
  LOG_IF(WARNING, !status.ok())
      << "Fail to start RemoteNode{id=" << id << " shard=" << ThisShardId()
      << "}: " << status.ToString();
}

Future<> RemoteNodeGroup::ShutdownNode(RemoteNodeUPtr node) {
  Gate::Holder holder(inflight_);
  if (!holder.ok()) {
    co_return;
  }
  co_await node->Shutdown();
}

RemoteNodeUPtr RemoteNodeGroup::TakeNodeOf(const CacheGroupMember& member) {
  const auto iter = nodes_.find(member.id);
  if (iter == nodes_.end() || iter->second == nullptr ||
      !IsSameEndpoint(iter->second->member(), member)) {
    return nullptr;
  }
  return std::move(iter->second);
}

bool RemoteNodeGroup::IsSameEndpoint(const CacheGroupMember& a,
                                     const CacheGroupMember& b) {
  return a.id == b.id && a.ip == b.ip && a.port == b.port;
}

}  // namespace blockcache
}  // namespace dingofs
