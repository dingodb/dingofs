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

#include <unordered_map>

#include "base/hash/ketama_con_hash.h"
#include "cache/utils/helper.h"

namespace dingofs {
namespace cache {

RemoteNodeGroup::RemoteNodeGroup(const PB_CacheGroupMembers& members)
    : members_(members),
      chash_(std::make_shared<base::hash::KetamaConHash>()) {}

// TODO: check node status, skip offline node
Status RemoteNodeGroup::Init() {
  auto weights = CalcWeights(members_);

  for (size_t i = 0; i < members_.size(); i++) {
    const auto& member = members_[i];
    auto key = MemberKey(member);
    auto node = std::make_shared<RemoteNodeImpl>(member, option_);
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

RemoteNodeSPtr RemoteNodeGroup::GetNode(const std::string& key) {
  base::hash::ConNode cnode;
  bool find = chash_->Lookup(key, cnode);
  CHECK(find);

  auto iter = nodes_.find(cnode.key);
  CHECK(iter != nodes_.end());
  return iter->second;
}

bool RemoteNodeGroup::IsDiff(const PB_CacheGroupMembers& members) const {
  std::unordered_map<uint64_t, PB_CacheGroupMember> m;
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

std::vector<uint64_t> RemoteNodeGroup::CalcWeights(
    const PB_CacheGroupMembers& members) {
  std::vector<uint64_t> weights(members.size());
  for (int i = 0; i < members.size(); i++) {
    weights[i] = members[i].weight();
  }
  return Helper::NormalizeByGcd(weights);
}

std::string RemoteNodeGroup::MemberKey(
    const PB_CacheGroupMember& member) const {
  return std::to_string(member.id());
}

//
}  // namespace cache
}  // namespace dingofs
