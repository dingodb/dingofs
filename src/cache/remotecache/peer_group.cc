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
 * Created Date: 2025-01-12
 * Author: Jingli Chen (Wine93)
 */

#include "cache/remotecache/peer_group.h"

#include <bthread/execution_queue.h>

#include <memory>
#include <string>
#include <unordered_map>

#include "cache/common/mds_client.h"
#include "cache/iutil/ketama_con_hash.h"
#include "cache/iutil/math_util.h"
#include "cache/remotecache/peer.h"

namespace dingofs {
namespace cache {

PeerGroupBuilder::PeerGroupBuilder()
    : old_group_(std::make_shared<PeerGroup>()) {
  bthread::ExecutionQueueOptions options;
  options.use_pthread = true;
  CHECK_EQ(0,
           bthread::execution_queue_start(
               &queue_id_, &options, &PeerGroupBuilder::ShutdownPeers, this));
}

PeerGroupBuilder::~PeerGroupBuilder() {
  CHECK_EQ(0, bthread::execution_queue_stop(queue_id_));
  CHECK_EQ(0, bthread::execution_queue_join(queue_id_));
}

PeerGroupSPtr PeerGroupBuilder::Build(const Members& members) {
  auto new_members = FilterMembers(members);
  auto diff = MakeDiff(new_members);
  if (diff.add.empty() && diff.remove.empty()) {
    return nullptr;
  } else if (new_members.empty()) {
    LOG(WARNING) << "No peers alive, skip building PeerGroup and use old one";
    return nullptr;
  }

  // Create a new PeerGroup
  std::vector<PeerSPtr> to_start, to_shutdown;
  std::unordered_map<std::string, PeerSPtr> new_peers;

  // kept peers
  for (const auto& old_peer : diff.keep) {
    new_peers[old_peer->Id()] = old_peer;
  }

  // added peers
  for (const auto& new_peer : diff.add) {
    new_peers[new_peer->Id()] = new_peer;
    to_start.emplace_back(new_peer);
  }

  // removed peers
  to_shutdown = std::move(diff.remove);

  StartPeers(to_start);
  DeferShutdownPeers(to_shutdown);

  auto group = std::make_shared<PeerGroup>();
  group->chash = BuildHashRing(new_members);
  group->peers = std::move(new_peers);

  old_group_ = group;
  return group;
}

Members PeerGroupBuilder::FilterMembers(const Members& members) {
  Members members_out;
  for (const auto& member : members) {
    if (member.state != CacheGroupMemberState::kOnline) {
      LOG(INFO) << "Filter out non-online " << member;
      continue;
    } else if (member.weight == 0) {
      LOG(INFO) << "Filter out zero-weight " << member;
      continue;
    }
    members_out.emplace_back(member);
  }
  return members_out;
}

PeerGroupBuilder::Diff PeerGroupBuilder::MakeDiff(const Members& new_members) {
  CHECK_NOTNULL(old_group_);

  Diff diff;
  std::unordered_set<std::string> new_mset;
  const auto& old_peers = old_group_->peers;

  // case 1: exist in new members
  for (const auto& new_member : new_members) {
    auto iter = old_peers.find(new_member.id);
    if (iter == old_peers.end()) {  // no found in old group
      diff.add.emplace_back(std::make_shared<Peer>(new_member.id, new_member.ip,
                                                   new_member.port));
    } else {  // found in old group
      auto old_peer = iter->second;
      if (old_peer->IP() == new_member.ip &&
          old_peer->Port() == new_member.port) {
        diff.keep.emplace_back(old_peer);
      } else {
        diff.remove.emplace_back(old_peer);
        diff.add.emplace_back(std::make_shared<Peer>(
            new_member.id, new_member.ip, new_member.port));
      }
    }
    new_mset.emplace(new_member.id);
  }

  // case 2: not exist in new members, it should be removed
  for (const auto& item : old_peers) {
    if (!new_mset.count(item.first)) {        // member_id
      diff.remove.emplace_back(item.second);  // peer
    }
  }

  return diff;
}

std::vector<uint64_t> PeerGroupBuilder::RecalcWeights(const Members& members) {
  std::vector<uint64_t> weights(members.size());
  for (int i = 0; i < members.size(); i++) {
    weights[i] = members[i].weight;
  }
  return iutil::NormalizeByGcd(weights);  // FIXME: uint32_t
}

iutil::ConHashUPtr PeerGroupBuilder::BuildHashRing(const Members& members) {
  auto chash = std::make_unique<iutil::KetamaConHash>();
  auto weights = RecalcWeights(members);
  for (int i = 0; i < members.size(); i++) {
    const auto& member = members[i];
    chash->AddNode(member.id, weights[i]);

    LOG(INFO) << "Add " << member << " to consistent hash";
  }

  chash->Final();
  LOG(INFO) << "Hash ring builded";
  return chash;
}

void PeerGroupBuilder::StartPeers(std::vector<PeerSPtr> peers) {
  for (const auto& peer : peers) {
    if (peer->Start().ok()) {
      LOG(INFO) << "Successfully started " << *peer;
    } else {
      LOG(ERROR) << "Fail to start " << *peer;
    }
  }
}

void PeerGroupBuilder::DeferShutdownPeers(std::vector<PeerSPtr> peers) {
  CHECK_EQ(0, bthread::execution_queue_execute(queue_id_, std::move(peers)));
}

int PeerGroupBuilder::ShutdownPeers(
    void*, bthread::TaskIterator<std::vector<PeerSPtr>>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  for (; iter; iter++) {
    for (const auto& peer : *iter) {
      peer->Shutdown();
      LOG(INFO) << "Successfully shutdown " << *peer;
    }
  }
  return 0;
}

}  // namespace cache
}  // namespace dingofs
