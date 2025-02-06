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

#ifndef DINGOFS_SRC_CACHE_CACHEGROUP_CLIENT_CACHE_GROUP_MEMBER_MANAGER_H_
#define DINGOFS_SRC_CACHE_CACHEGROUP_CLIENT_CACHE_GROUP_MEMBER_MANAGER_H_

#include <cstdint>
#include <memory>

#include "base/hash/con_hash.h"
#include "client/cachegroup/client/cache_group_node.h"
#include "dingofs/cachegroup.pb.h"
#include "stub/rpcclient/mds_client.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace cache {
namespace cachegroup {

using ::dingofs::base::hash::ConHash;
using ::dingofs::base::timer::TimerImpl;
using ::dingofs::pb::mds::cachegroup::CacheGroupMember;
using ::dingofs::stub::rpcclient::MdsClient;
using ::dingofs::utils::RWLock;

class CacheGroupMemberManager {
  using NodesT =
      std::unordered_map<std::string, std::shared_ptr<CacheGroupNode>>;

 public:
  CacheGroupMemberManager(const std::string& group_name,
                          uint32_t load_interval_ms,
                          std::shared_ptr<MdsClient> mds_client);

  bool Start();

  std::shared_ptr<CacheGroupNode> GetCacheGroupNode(const std::string& key);

 private:
  bool LoadMembers();

  bool FetchRemoteMembers(std::vector<CacheGroupMember>* members);

  bool IsSame(const std::vector<CacheGroupMember>& remote_members);

  std::vector<uint64_t> CalcWeights(
      const std::vector<CacheGroupMember>& members);

  std::shared_ptr<ConHash> BuildHash(
      const std::vector<CacheGroupMember>& members);

  std::shared_ptr<NodesT> CreateNodes(
      const std::vector<CacheGroupMember>& members);

 private:
  RWLock rwlock_;  // for chash_ & nodes_
  std::string group_name_;
  uint32_t load_interval_ms_;
  std::shared_ptr<MdsClient> mds_client_;
  std::unique_ptr<TimerImpl> timer_;
  std::shared_ptr<ConHash> chash_;
  std::shared_ptr<NodesT> nodes_;
};

}  // namespace cachegroup
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_CLIENT_CACHE_GROUP_MEMBER_MANAGER_H_
