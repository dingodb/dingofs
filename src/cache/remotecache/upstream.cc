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
 * Created Date: 2025-07-31
 * Author: Jingli Chen (Wine93)
 */

#include "cache/remotecache/upstream.h"

#include <butil/time.h>
#include <glog/logging.h>

#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>

#include "cache/blockcache/cache_store.h"
#include "cache/common/mds_client.h"
#include "cache/common/type.h"
#include "cache/metric/cache_status.h"
#include "cache/remotecache/remote_cache_node_impl.h"
#include "cache/utils/helper.h"
#include "cache/utils/ketama_con_hash.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

static std::ostream& operator<<(std::ostream& os,
                                const std::vector<CacheGroupMember>& members) {
  os << "[\n";
  for (const auto& member : members) {
    os << "  " << member.ToString() << "\n";
  }
  os << "]";
  return os;
}

ConsistentHash::ConsistentHash() : chash_(std::make_unique<KetamaConHash>()) {}

void ConsistentHash::Build(const std::vector<CacheGroupMember>& members) {
  LOG(INFO) << "Building consistent hash with " << members.size()
            << " members: " << members;

  if (members.empty()) {
    LOG(WARNING) << "No valid members provide, skip build consistent hash.";
    return;
  }

  auto weights = CalcWeights(members);
  for (size_t i = 0; i < members.size(); i++) {
    const auto& member = members[i];
    const auto& key = member.id;
    auto node = std::make_shared<RemoteCacheNodeImpl>(member);
    nodes_[key] = node;
    chash_->AddNode(key, weights[i]);

    LOG(INFO) << "Add cache group member (" << member.ToString()
              << ") to consistent hash success.";
  }

  chash_->Final();
}

RemoteCacheNodeSPtr ConsistentHash::GetNode(const std::string& key) {
  if (nodes_.empty()) {
    return nullptr;
  }

  ConNode cnode;
  bool find = chash_->Lookup(key, cnode);
  CHECK(find) << "No chash node found for key: " << key;

  auto iter = nodes_.find(cnode.key);
  CHECK(iter != nodes_.end()) << "No remote cache node found for key: " << key;
  return iter->second;
}

ConsistentHash::NodesT& ConsistentHash::GetAllNodes() { return nodes_; }

const ConsistentHash::NodesT& ConsistentHash::GetAllNodes() const {
  return nodes_;
}

std::vector<uint64_t> ConsistentHash::CalcWeights(
    const std::vector<CacheGroupMember>& members) {
  std::vector<uint64_t> weights(members.size());
  for (int i = 0; i < members.size(); i++) {
    weights[i] = members[i].weight;
  }
  return Helper::NormalizeByGcd(weights);
}

UpstreamImpl::UpstreamImpl() : chash_(std::make_shared<ConsistentHash>()) {}

void UpstreamImpl::Build(const std::vector<CacheGroupMember>& members) {
  CHECK_NOTNULL(chash_);

  VLOG(1) << "Build upstream with " << members.size()
          << " members: " << members;

  butil::Timer timer;
  timer.start();

  // Check change: filter out non-online members
  const auto& old_members = members_;
  const auto& new_members = FilterMember(members);
  if (IsSame(old_members, new_members)) {
    return;
  }

  // Build consistent hash with new members
  auto old_chash = chash_;
  auto new_chash = std::make_shared<ConsistentHash>();
  new_chash->Build(new_members);

  // Share nodes and start new nodes
  const auto& old_nodes = old_chash->GetAllNodes();
  auto& new_nodes = new_chash->GetAllNodes();
  auto diff = ShareNodes(old_nodes, new_nodes);
  StartNodes(diff.add_nodes);

  // Reset consistent hash
  ResetCHash(new_members, new_chash);

  // Shutdown old nodes
  ShutdownNodes(diff.remove_nodes);

  SetStatusPage();

  timer.stop();
  LOG(INFO) << "Build upstream success: old_members (" << old_members.size()
            << "), new_members (" << new_members.size() << "), cost "
            << timer.u_elapsed() * 1.0 / 1e6 << " seconds";
}

Status UpstreamImpl::GetNode(const BlockKey& key, RemoteCacheNodeSPtr& node) {
  ReadLockGuard lock(rwlock_);
  node = chash_->GetNode(key.Filename());
  if (nullptr == node) {
    return Status::NotFound("no remote cache node available");
  }
  return Status::OK();
}

std::vector<CacheGroupMember> UpstreamImpl::FilterMember(
    const std::vector<CacheGroupMember>& members) {
  std::vector<CacheGroupMember> members_out;
  for (const auto& member : members) {
    if (member.state != CacheGroupMemberState::kOnline) {
      LOG(INFO) << "Skip non-online cache group member: member = "
                << member.ToString();
      continue;
    } else if (member.weight == 0) {
      LOG(INFO) << "Skip cache group member with zero weight: member = "
                << member.ToString();
      continue;
    }
    members_out.emplace_back(member);
  }
  return members_out;
}

bool UpstreamImpl::IsSame(
    const std::vector<CacheGroupMember>& old_members,
    const std::vector<CacheGroupMember>& new_members) const {
  if (old_members.size() != new_members.size()) {
    return false;
  }

  std::unordered_map<std::string, CacheGroupMember> m;
  for (const auto& member : old_members) {
    m[member.id] = member;
  }

  for (const auto& member : new_members) {
    auto iter = m.find(member.id);
    if (iter == m.end() || !(iter->second == member)) {
      return false;
    }
  }
  return true;
}

UpstreamImpl::Diff UpstreamImpl::ShareNodes(
    const ConsistentHash::NodesT& old_nodes,
    ConsistentHash::NodesT& new_nodes) {
  Diff diff;
  for (const auto& new_node : new_nodes) {
    auto iter = old_nodes.find(new_node.first);
    if (iter == old_nodes.end()) {  // not found in old nodes
      diff.add_nodes.push_back(new_node.second);
    } else {
      new_nodes[new_node.first] = iter->second;  // keep old node
    }
  }

  for (const auto& old_node : old_nodes) {
    // not found in new nodes
    if (new_nodes.find(old_node.first) == new_nodes.end()) {
      diff.remove_nodes.push_back(old_node.second);
    }
  }
  return diff;
}

void UpstreamImpl::StartNodes(
    const std::vector<RemoteCacheNodeSPtr>& add_nodes) {
  LOG(INFO) << "Start " << add_nodes.size() << " added remote cache nodes";

  for (const auto& node : add_nodes) {
    auto status = node->Start();
    if (!status.ok()) {  // NOTE: only throw error
      LOG(ERROR) << "Start remote cache node failed: " << status.ToString();
    }
  }
}

void UpstreamImpl::ShutdownNodes(
    const std::vector<RemoteCacheNodeSPtr>& remove_nodes) {
  LOG(INFO) << "Shutdown " << remove_nodes.size()
            << " removed remote cache nodes";

  for (const auto& node : remove_nodes) {
    auto status = node->Shutdown();
    if (!status.ok()) {  // NOTE: only throw error
      LOG(ERROR) << "Shutdown remote cache node failed: " << status.ToString();
    }
  }
}

void UpstreamImpl::ResetCHash(const std::vector<CacheGroupMember>& new_members,
                              ConsistentHashSPtr new_chash) {
  WriteLockGuard lock(rwlock_);
  members_ = new_members;
  chash_ = new_chash;
}

void UpstreamImpl::SetStatusPage() const {
  CacheStatus::Update([this](CacheStatus::Root& root) {
    auto& nodes = root.remote_cache.nodes;
    nodes.clear();
    for (const auto& member : members_) {
      nodes[member.id] = CacheStatus::Node{
          .id = member.id,
          .address = absl::StrFormat("%s:%d", member.ip, member.port),
          .weight = member.weight,
          .state =
              Helper::ToLowerCase(CacheGroupMemberStateToString(member.state)),
          .health = "normal",  // FIXME(P0)
      };
    }
    root.remote_cache.last_modified = Helper::NowTime();
  });
}

}  // namespace cache
}  // namespace dingofs
