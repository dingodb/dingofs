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

#ifndef DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_CACHE_NODE_MANAGER_H_
#define DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_CACHE_NODE_MANAGER_H_

#include "cache/common/proto.h"
#include "common/status.h"
#include "options/cache/tiercache.h"
#include "stub/rpcclient/mds_client.h"
#include "utils/executor/executor.h"

namespace dingofs {
namespace cache {

using OnMemberLoad = std::function<void(const PBCacheGroupMembers& members)>;

class RemoteCacheNodeManager {
 public:
  RemoteCacheNodeManager(RemoteBlockCacheOption option,
                         OnMemberLoad on_member_load);

  Status Start();
  void Shutdown();

 private:
  void BackgroudRefresh();

  Status RefreshMembers();

  Status LoadMembers(PBCacheGroupMembers* members);

  // void ExposeInfo() const;

  std::atomic<bool> running_;
  RemoteBlockCacheOption option_;
  OnMemberLoad on_member_load_;
  std::shared_ptr<stub::rpcclient::MDSBaseClient> mds_base_;
  std::shared_ptr<stub::rpcclient::MdsClient> mds_client_;
  std::unique_ptr<Executor> executor_;
};

using RemoteCacheNodeManagerUPtr = std::unique_ptr<RemoteCacheNodeManager>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_CACHE_NODE_MANAGER_H_
