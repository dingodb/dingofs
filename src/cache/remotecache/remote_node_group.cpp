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

#include "cache/remotecache/remote_node_group.h"

#include <memory>

#include "base/hash/ketama_con_hash.h"
#include "cache/blockcache/cache_store.h"
#include "cache/config/remote_cache.h"
#include "cache/remotecache/remote_node_impl.h"
#include "cache/remotecache/remote_node_manager.h"
#include "cache/utils/helper.h"

namespace dingofs {
namespace cache {

Upstream::Upstream() : chash_(std::make_shared<base::hash::KetamaConHash>()) {}

Upstream::Upstream(const PBCacheGroupMembers& members,
                   RemoteBlockCacheOption option)
    : members_(members),
      option_(option),
      chash_(std::make_shared<base::hash::KetamaConHash>()) {}

// TODO: check node status, skip offline node
Status Upstream::Init() {
  auto weights = CalcWeights(members_);

  for (size_t i = 0; i < members_.size(); i++) {
    const auto& member = members_[i];
    auto key = MemberKey(member);
    auto node =
        std::make_shared<RemoteNodeImpl>(member, option_.remote_node_option);
    auto status = node->Init();
    if (!status.ok()) {  // NOTE: only throw error
      LOG(ERROR) << "Init remote node failed: id = " << member.id()
                 << ", status = " << status.ToString();
    }

    nodes_[key] = node;
    chash_->AddNode(key, weights[i]);

    LOG(INFO) << "Add cache group member (id=" << member.id()
              << ", endpoint=" << member.ip() << ":" << member.port()
              << ", weight=" << weights[i] << ") to cache group success.";
  }

  chash_->Final();
  return Status::OK();
}

RemoteNodeSPtr Upstream::GetNode(const std::string& key) {
  base::hash::ConNode cnode;
  bool find = chash_->Lookup(key, cnode);
  CHECK(find);

  auto iter = nodes_.find(cnode.key);
  CHECK(iter != nodes_.end());
  return iter->second;
}

bool Upstream::IsDiff(const PBCacheGroupMembers& members) const {
  std::unordered_map<uint64_t, PBCacheGroupMember> m;
  for (const auto& member : members_) {
    m[member.id()] = member;
  }

  for (const auto& member : members) {
    auto iter = m.find(member.id());
    if (iter == m.end() || !(iter->second == member)) {
      return true;  // different member found
    }
  }

  return false;
}

std::vector<uint64_t> Upstream::CalcWeights(
    const PBCacheGroupMembers& members) {
  std::vector<uint64_t> weights(members.size());
  for (int i = 0; i < members.size(); i++) {
    weights[i] = members[i].weight();
  }
  return Helper::NormalizeByGcd(weights);
}

std::string Upstream::MemberKey(const PBCacheGroupMember& member) const {
  return std::to_string(member.id());
}

RemoteNodeGroup::RemoteNodeGroup(RemoteBlockCacheOption option)
    : running_(false),
      option_(option),
      upstream_(std::make_shared<Upstream>()) {
  node_manager_ = std::make_unique<RemoteNodeManager>(
      option, [this](const PBCacheGroupMembers& members) {
        return OnMemberLoad(members);
      });
}

Status RemoteNodeGroup::Init() {
  if (!running_.exchange(true)) {
    return node_manager_->Start();
  }
  return Status::OK();
}

Status RemoteNodeGroup::Destroy() {
  if (running_.exchange(false)) {
    node_manager_->Stop();
  }
  return Status::OK();
}

Status RemoteNodeGroup::Put(const BlockKey& key, const Block& block) {
  return GetNode(key)->Put(key, block);
}

Status RemoteNodeGroup::Range(const BlockKey& key, off_t offset, size_t length,
                              IOBuffer* buffer, size_t block_size) {
  return GetNode(key)->Range(key, offset, length, buffer, block_size);
}

Status RemoteNodeGroup::Cache(const BlockKey& key, const Block& block) {
  return GetNode(key)->Cache(key, block);
}

Status RemoteNodeGroup::Prefetch(const BlockKey& key, size_t length) {
  return GetNode(key)->Prefetch(key, length);
}

RemoteNodeSPtr RemoteNodeGroup::GetNode(const BlockKey& key) {
  ReadLockGuard lock(rwlock_);
  CHECK_NOTNULL(upstream_);
  return upstream_->GetNode(key.Filename());
}

Status RemoteNodeGroup::OnMemberLoad(const PBCacheGroupMembers& members) {
  if (!upstream_->IsDiff(members)) {
    return Status::OK();
  }

  auto upstream = std::make_shared<Upstream>(members, option_);
  auto status = upstream->Init();
  if (!status.ok()) {
    return status;
  }

  {
    WriteLockGuard lock(rwlock_);
    upstream_ = upstream;
  }
  return Status::OK();
}

}  // namespace cache
}  // namespace dingofs
