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

#ifndef DINGOFS_SRC_CACHE_REMOTECACHE_UPSTREAM_H_
#define DINGOFS_SRC_CACHE_REMOTECACHE_UPSTREAM_H_

#include <memory>

#include "cache/blockcache/cache_store.h"
#include "cache/common/proto.h"
#include "cache/common/type.h"
#include "cache/remotecache/remote_cache_node.h"
#include "cache/utils/con_hash.h"

namespace dingofs {
namespace cache {

class ConsistentHash {
 public:
  using NodesT = std::unordered_map<std::string, RemoteCacheNodeSPtr>;

  ConsistentHash();

  void Build(const PBCacheGroupMembers& members);

  RemoteCacheNodeSPtr GetNode(const std::string& key);

  NodesT& GetAllNodes();
  const NodesT& GetAllNodes() const;

 private:
  std::vector<uint64_t> CalcWeights(const PBCacheGroupMembers& members);
  std::string MemberKey(const PBCacheGroupMember& member) const;

  std::unique_ptr<ConHash> chash_;
  NodesT nodes_;
};

using ConsistentHashSPtr = std::shared_ptr<ConsistentHash>;

class Upstream {
 public:
  virtual ~Upstream() = default;

  virtual void Build(const PBCacheGroupMembers& members) = 0;

  virtual Status GetNode(const BlockKey& key, RemoteCacheNodeSPtr& node) = 0;
};

class UpstreamImpl : public Upstream {
 public:
  UpstreamImpl();

  void Build(const PBCacheGroupMembers& members) override;

  Status GetNode(const BlockKey& key, RemoteCacheNodeSPtr& node) override;

 private:
  struct Diff {
    std::vector<RemoteCacheNodeSPtr> add_nodes;
    std::vector<RemoteCacheNodeSPtr> remove_nodes;
  };

  PBCacheGroupMembers FilterMember(const PBCacheGroupMembers& members);

  bool IsSame(const PBCacheGroupMembers& old_members,
              const PBCacheGroupMembers& new_members) const;

  Diff ShareNodes(const ConsistentHash::NodesT& old_nodes,
                  ConsistentHash::NodesT& new_nodes);

  void StartNodes(const std::vector<RemoteCacheNodeSPtr>& add_nodes);
  void ShutdownNodes(const std::vector<RemoteCacheNodeSPtr>& remove_nodes);

  void ResetCHash(const PBCacheGroupMembers& new_members,
                  ConsistentHashSPtr new_chash);

  void ExposeOnUpstreamChange();

  BthreadRWLock rwlock_;
  PBCacheGroupMembers members_;
  ConsistentHashSPtr chash_;
};

using UpstreamUPtr = std::unique_ptr<UpstreamImpl>;

};  // namespace cache
};  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_UPSTREAM_H_
