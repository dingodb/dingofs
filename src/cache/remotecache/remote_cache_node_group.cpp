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

#include "cache/remotecache/remote_cache_node_group.h"

#include <glog/logging.h>

#include <memory>

#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/cache_store.h"
#include "cache/common/macro.h"
#include "cache/remotecache/remote_cache_node.h"
#include "cache/remotecache/remote_cache_node_impl.h"
#include "cache/remotecache/remote_cache_node_manager.h"
#include "cache/utils/context.h"
#include "cache/utils/helper.h"
#include "cache/utils/ketama_con_hash.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

static bool operator==(const PBCacheGroupMember& lhs,
                       const PBCacheGroupMember& rhs) {
  return lhs.id() == rhs.id() && lhs.ip() == rhs.ip() &&
         lhs.port() == rhs.port() && lhs.weight() == rhs.weight() &&
         lhs.state() == rhs.state();
}

CacheUpstream::CacheUpstream() : chash_(std::make_shared<KetamaConHash>()) {}

CacheUpstream::CacheUpstream(const PBCacheGroupMembers& members,
                             RemoteBlockCacheOption option)
    : members_(members),
      option_(option),
      chash_(std::make_shared<KetamaConHash>()) {}

Status CacheUpstream::Init() {
  auto weights = CalcWeights(members_);

  for (size_t i = 0; i < members_.size(); i++) {
    const auto& member = members_[i];
    if (member.state() !=
        PBCacheGroupMemberState::CacheGroupMemberStateOnline) {
      LOG(INFO) << "Skip non-online cache group member: id = " << member.id()
                << ", endpoint = " << member.ip() << ":" << member.port();
      continue;
    } else if (member.weight() == 0) {
      LOG(INFO) << "Skip cache group member with zero weight: id = "
                << member.id() << ", endpoint = " << member.ip() << ":"
                << member.port();
      continue;
    }

    auto key = MemberKey(member);
    auto node = std::make_shared<RemoteCacheNodeImpl>(member, option_);
    auto status = node->Start();
    if (!status.ok()) {  // NOTE: only throw error
      LOG(ERROR) << "Init remote node failed: id = " << member.id()
                 << ", status = " << status.ToString();
    }

    nodes_[key] = node;
    chash_->AddNode(key, weights[i]);

    LOG(INFO) << "Add cache group member (id=" << member.id()
              << ",endpoint=" << member.ip() << ":" << member.port()
              << ",weight=" << weights[i] << ",state=" << member.state()
              << ") to cache group success.";
  }

  chash_->Final();
  return Status::OK();
}

RemoteCacheNodeSPtr CacheUpstream::GetNode(const std::string& key) {
  ConNode cnode;
  bool find = chash_->Lookup(key, cnode);
  CHECK(find);

  auto iter = nodes_.find(cnode.key);
  CHECK(iter != nodes_.end());
  return iter->second;
}

bool CacheUpstream::IsDiff(const PBCacheGroupMembers& members) const {
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

bool CacheUpstream::IsEmpty() const { return nodes_.empty(); }

std::vector<uint64_t> CacheUpstream::CalcWeights(
    const PBCacheGroupMembers& members) {
  std::vector<uint64_t> weights(members.size());
  for (int i = 0; i < members.size(); i++) {
    weights[i] = members[i].weight();
  }
  return Helper::NormalizeByGcd(weights);
}

std::string CacheUpstream::MemberKey(const PBCacheGroupMember& member) const {
  return std::to_string(member.id());
}

RemoteCacheNodeGroup::RemoteCacheNodeGroup(RemoteBlockCacheOption option)
    : running_(false),
      option_(option),
      upstream_(std::make_shared<CacheUpstream>()),
      metric_(std::make_shared<RemoteCacheCacheNodeGroupMetric>()) {
  node_manager_ = std::make_unique<RemoteCacheNodeManager>(
      option, [this](const PBCacheGroupMembers& members) {
        return OnMemberLoad(members);
      });
}

Status RemoteCacheNodeGroup::Start() {
  CHECK_NOTNULL(upstream_);
  CHECK_NOTNULL(node_manager_);
  CHECK_NOTNULL(metric_);

  if (running_) {
    return Status::OK();
  }

  LOG(INFO) << "Remote node group is starting...";

  auto status = node_manager_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Start remote node manager failed: " << status.ToString();
    return status;
  }

  running_ = true;

  LOG(INFO) << "Remote node group is up.";

  CHECK_RUNNING("Remote node group");
  return Status::OK();
}

Status RemoteCacheNodeGroup::Shutdown() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }

  LOG(INFO) << "Remote node group is shutting down...";

  node_manager_->Shutdown();

  LOG(INFO) << "Remote node group is down.";

  CHECK_DOWN("Remote node group");
  return Status::OK();
}

Status RemoteCacheNodeGroup::Put(ContextSPtr ctx, const BlockKey& key,
                                 const Block& block) {
  CHECK_RUNNING("Remote node group");

  Status status;
  RemoteCacheNodeSPtr node;
  RemoteCacheCacheNodeGroupMetricGuard metirc_guard(__func__, block.size,
                                                    status, metric_);
  status = GetNode(key, node);
  if (status.ok()) {
    status = node->Put(ctx, key, block);
  }

  return status;
}

Status RemoteCacheNodeGroup::Range(ContextSPtr ctx, const BlockKey& key,
                                   off_t offset, size_t length,
                                   IOBuffer* buffer, RangeOption option) {
  CHECK_RUNNING("Remote node group");

  Status status;
  RemoteCacheNodeSPtr node;
  RemoteCacheCacheNodeGroupMetricGuard metric_guard(__func__, length, status,
                                                    metric_);
  status = GetNode(key, node);
  if (status.ok()) {
    status = node->Range(ctx, key, offset, length, buffer, option);
  }

  return status;
}

Status RemoteCacheNodeGroup::Cache(ContextSPtr ctx, const BlockKey& key,
                                   const Block& block) {
  CHECK_RUNNING("Remote node group");

  Status status;
  RemoteCacheNodeSPtr node;
  RemoteCacheCacheNodeGroupMetricGuard metric_guard(__func__, block.size,
                                                    status, metric_);
  status = GetNode(key, node);
  if (status.ok()) {
    status = node->Cache(ctx, key, block);
  }

  return status;
}

Status RemoteCacheNodeGroup::Prefetch(ContextSPtr ctx, const BlockKey& key,
                                      size_t length) {
  CHECK_RUNNING("Remote node group");

  Status status;
  RemoteCacheNodeSPtr node;
  RemoteCacheCacheNodeGroupMetricGuard metric_guard(__func__, length, status,
                                                    metric_);
  status = GetNode(key, node);
  if (status.ok()) {
    status = node->Prefetch(ctx, key, length);
  }

  return status;
}

Status RemoteCacheNodeGroup::GetNode(const BlockKey& key,
                                     RemoteCacheNodeSPtr& node) {
  ReadLockGuard lock(rwlock_);
  if (upstream_->IsEmpty()) {
    return Status::NotFound("no cache node available");
  }

  node = upstream_->GetNode(key.Filename());
  return Status::OK();
}

Status RemoteCacheNodeGroup::OnMemberLoad(const PBCacheGroupMembers& members) {
  if (!upstream_->IsDiff(members)) {
    return Status::OK();
  }

  auto upstream = std::make_shared<CacheUpstream>(members, option_);
  auto status = upstream->Init();
  if (!status.ok()) {
    LOG(ERROR) << "Init cache upstream failed: " << status.ToString();
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
