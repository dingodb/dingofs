
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
 * Created Date: 2026-01-12
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_REMOTECACHE_PEER_GROUP_H_
#define DINGOFS_SRC_CACHE_REMOTECACHE_PEER_GROUP_H_

#include <bthread/execution_queue.h>
#include <bthread/execution_queue_inl.h>

#include <memory>
#include <unordered_map>
#include <vector>

#include "cache/common/mds_client.h"
#include "cache/iutil/con_hash.h"
#include "cache/remotecache/peer.h"

namespace dingofs {
namespace cache {

struct PeerGroup {
  PeerSPtr SelectPeer(const std::string& key) {
    iutil::ConNode node;
    if (chash->Lookup(key, node)) {
      auto iter = peers.find(node.key);
      if (iter != peers.end()) {
        return iter->second;
      }
    }
    return nullptr;
  }

  std::unique_ptr<iutil::ConHash> chash;            // member id => vnode
  std::unordered_map<std::string, PeerSPtr> peers;  // member id => Peer*
};

using PeerGroupSPtr = std::shared_ptr<PeerGroup>;

class PeerGroupBuilder {
 public:
  PeerGroupBuilder();
  ~PeerGroupBuilder();

  PeerGroupSPtr Build(const Members& members);

 private:
  struct Diff {
    std::vector<PeerSPtr> keep;
    std::vector<PeerSPtr> add;
    std::vector<PeerSPtr> remove;
  };

  Members FilterMembers(const Members& members);
  Diff MakeDiff(const Members& new_members);
  std::vector<uint64_t> RecalcWeights(const Members& members);
  iutil::ConHashUPtr BuildHashRing(const Members& members);

  void StartPeers(std::vector<PeerSPtr> peers);
  void DeferShutdownPeers(std::vector<PeerSPtr> peers);
  static int ShutdownPeers(void* meta,
                           bthread::TaskIterator<std::vector<PeerSPtr>>& iter);

  PeerGroupSPtr old_group_;
  bthread::ExecutionQueueId<std::vector<PeerSPtr>> queue_id_;
};

using PeerGroupBuilderUPtr = std::unique_ptr<PeerGroupBuilder>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_PEER_GROUP_H_
