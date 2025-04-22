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

#ifndef DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_NODE_GROUP_H_
#define DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_NODE_GROUP_H_

#include <cstdint>
#include <memory>

#include "base/hash/con_hash.h"
#include "cache/common/common.h"
#include "cache/remotecache/remote_node.h"
#include "dingofs/cachegroup.pb.h"
#include "stub/rpcclient/mds_client.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace cache {
namespace remotecache {

using dingofs::base::hash::ConHash;
using dingofs::base::timer::TimerImpl;
using dingofs::pb::mds::cachegroup::CacheGroupMember;
using dingofs::stub::rpcclient::MdsClient;
using dingofs::utils::RWLock;

class RemoteNodeGroup {
 public:
  virtual ~RemoteNodeGroup() = default;

  virtual Status Start() = 0;

  virtual void Stop() = 0;

  virtual RemoteNodePtr Get(const std::string& key) = 0;
};

class RemoteNodeGroupImpl : public RemoteNodeGroup {
  using NodesT = std::unordered_map<std::string, std::shared_ptr<RemoteNode>>;
  using CacheGroupMembers = std::vector<CacheGroupMember>;

 public:
  RemoteNodeGroupImpl(RemoteBlockCacheOption option,
                      std::shared_ptr<MdsClient> mds_client);

  ~RemoteNodeGroupImpl() override = default;

  Status Start() override;

  void Stop() override;

  RemoteNodePtr Get(const std::string& key) override;

 private:
  Status RefreshMembers();

  Status LoadMembers(CacheGroupMembers* members);

  static bool IsSame(const CacheGroupMembers& local_members,
                     const CacheGroupMembers& remote_members);

  void CommitChange(const CacheGroupMembers& members);

  std::vector<uint64_t> CalcWeights(const CacheGroupMembers& members);

  std::shared_ptr<ConHash> BuildHash(const CacheGroupMembers& members);

  std::shared_ptr<NodesT> CreateNodes(const CacheGroupMembers& members);

 private:
  RWLock rwlock_;  // for chash_ & nodes_
  std::atomic<bool> running_;
  RemoteBlockCacheOption option_;
  std::shared_ptr<MdsClient> mds_client_;
  CacheGroupMembers local_members_;
  std::unique_ptr<TimerImpl> timer_;
  std::shared_ptr<ConHash> chash_;
  std::shared_ptr<NodesT> nodes_;
};

}  // namespace remotecache
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_NODE_GROUP_H_
