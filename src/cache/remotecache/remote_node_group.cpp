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
 * Created Date: 2025-01-13
 * Author: Jingli Chen (Wine93)
 */

#include "cache/remotecache/remote_node_group.h"

#include <glog/logging.h>

#include <memory>
#include <unordered_map>

#include "base/hash/con_hash.h"
#include "base/hash/ketama_con_hash.h"
#include "cache/remotecache/remote_node.h"
#include "common/status.h"
#include "dingofs/cachegroup.pb.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace cache {
namespace remotecache {

using dingofs::base::hash::ConNode;
using dingofs::base::hash::KetamaConHash;
using dingofs::pb::mds::cachegroup::CacheGroupErrCode_Name;
using dingofs::pb::mds::cachegroup::CacheGroupOk;
using dingofs::utils::ReadLockGuard;
using dingofs::utils::WriteLockGuard;

bool operator==(const CacheGroupMember& a, const CacheGroupMember& b) {
  return a.id() == b.id() && a.ip() == b.ip() && a.port() == b.port() &&
         a.weight() == b.weight();
}

RemoteNodeGroupImpl::RemoteNodeGroupImpl(RemoteBlockCacheOption option,
                                         std::shared_ptr<MdsClient> mds_client)
    : running_(false),
      option_(option),
      mds_client_(mds_client),
      timer_(std::make_unique<TimerImpl>()),
      chash_(std::make_shared<KetamaConHash>()),
      nodes_(std::make_shared<NodesT>()) {}

Status RemoteNodeGroupImpl::Start() {
  if (!running_.exchange(true)) {
    Status status = RefreshMembers();
    if (!status.ok()) {
      LOG(ERROR) << "Load cache group members failed.";
      return status;
    }

    CHECK(timer_->Start());
    timer_->Add([this] { RefreshMembers(); },
                option_.load_members_interval_ms());
  }

  return Status::OK();
}

void RemoteNodeGroupImpl::Stop() {
  if (running_.exchange(false)) {
  }
}

std::shared_ptr<RemoteNode> RemoteNodeGroupImpl::Get(const std::string& key) {
  ReadLockGuard lk(rwlock_);

  ConNode cnode;
  bool find = chash_->Lookup(key, cnode);
  CHECK(find);

  auto iter = nodes_->find(cnode.key);
  CHECK(iter != nodes_->end());
  return iter->second;
}

Status RemoteNodeGroupImpl::RefreshMembers() {
  std::vector<CacheGroupMember> remote_members;
  Status status = LoadMembers(&remote_members);
  if (!status.ok()) {
    return status;
  }

  if (!IsSame(local_members_, remote_members)) {
    CommitChange(remote_members);
  }
  return Status::OK();
}

Status RemoteNodeGroupImpl::LoadMembers(CacheGroupMembers* members) {
  auto status =
      mds_client_->LoadCacheGroupMembers(option_.group_name(), members);
  if (status != CacheGroupOk) {
    LOG(ERROR) << "Load cache group members failed: "
               << CacheGroupErrCode_Name(status);
    return Status::Internal("load cache group member failed");
  }
  return Status::OK();
}

bool RemoteNodeGroupImpl::IsSame(const CacheGroupMembers& local_members,
                                 const CacheGroupMembers& remote_members) {
  if (local_members.size() != remote_members.size()) {
    return false;
  }

  std::unordered_map<uint64_t, CacheGroupMember> local;
  for (const CacheGroupMember& member : local_members) {
    local[member.id()] = member;
  }

  for (const CacheGroupMember& member : remote_members) {
    auto iter = local.find(member.id());
    if (iter == local.end()) {
      return false;
    } else if (!(member == iter->second)) {
      return false;
    }
  }
  return true;
}

void RemoteNodeGroupImpl::CommitChange(const CacheGroupMembers& members) {
  auto chash = BuildHash(members);
  auto nodes = CreateNodes(members);

  // commit latest cache group members
  WriteLockGuard lk(rwlock_);
  local_members_ = members;
  chash_ = chash;
  nodes_ = nodes;
  LOG(INFO) << "remote node group changed.";
}

std::vector<uint64_t> RemoteNodeGroupImpl::CalcWeights(
    const CacheGroupMembers& members) {
  uint64_t gcd = 0;
  std::vector<uint64_t> weights;
  for (const auto& member : members) {
    weights.push_back(member.weight());
    gcd = std::gcd(gcd, member.weight());
  }
  CHECK_NE(gcd, 0);

  for (auto& weight : weights) {
    weight = weight / gcd;
  }
  return weights;
}

std::shared_ptr<ConHash> RemoteNodeGroupImpl::BuildHash(
    const CacheGroupMembers& members) {
  auto weights = CalcWeights(members);
  CHECK_EQ(members.size(), weights.size());

  auto chash = std::make_shared<KetamaConHash>();
  for (size_t i = 0; i < members.size(); i++) {
    const auto& member = members[i];
    chash->AddNode(std::to_string(member.id()), weights[i]);
    LOG(INFO) << "Add cache group member (id=" << member.id()
              << ", endpoint=" << member.ip() << ":" << member.port()
              << ", weight=" << weights[i] << ") to cache group success.";
  }

  chash->Final();
  return chash;
}

std::shared_ptr<RemoteNodeGroupImpl::NodesT> RemoteNodeGroupImpl::CreateNodes(
    const CacheGroupMembers& members) {
  auto nodes = std::make_shared<NodesT>();
  for (const auto& member : members) {
    auto node =
        std::make_shared<RemoteNodeImpl>(member, option_.remote_node_option());

    auto status = node->Init();
    if (status.ok()) {  // NOTE: only throw warning
      LOG(ERROR) << "Init remote node failed, id = " << member.id()
                 << ", status = " << status.ToString();
    }

    nodes->emplace(std::to_string(member.id()), node);
  }
  return nodes;
}

}  // namespace remotecache
}  // namespace cache
}  // namespace dingofs
