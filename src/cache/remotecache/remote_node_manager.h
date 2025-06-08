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

#ifndef DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_NODE_MANAGER_H_
#define DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_NODE_MANAGER_H_

#include "cache/common/common.h"
#include "cache/remotecache/remote_node_group.h"
#include "stub/rpcclient/mds_client.h"
#include "utils/executor/timer.h"

namespace dingofs {
namespace cache {

class RemoteNodeManager {
 public:
  virtual ~RemoteNodeManager() = default;

  virtual Status Start() = 0;
  virtual void Stop() = 0;

  virtual RemoteNodeSPtr GetNode(const std::string& key) = 0;
};

using RemoteNodeManagerUPtr = std::unique_ptr<RemoteNodeManager>;

class RemoteNodeManagerImpl final : public RemoteNodeManager {
 public:
  explicit RemoteNodeManagerImpl(RemoteBlockCacheOption option);
  ~RemoteNodeManagerImpl() override = default;

  Status Start() override;
  void Stop() override;

  RemoteNodeSPtr GetNode(const std::string& key) override;

 private:
  void BackgroudRefresh();
  Status RefreshMembers();
  Status LoadMembers(PB_CacheGroupMembers* members);

  RemoteNodeGorupSPtr GetGroup();
  void SetGroup(RemoteNodeGorupSPtr group);

  std::atomic<bool> running_;
  BthreadRWLock rwlock_;  // for group
  const RemoteBlockCacheOption option_;
  std::shared_ptr<stub::rpcclient::MDSBaseClient> mds_base_;
  std::shared_ptr<stub::rpcclient::MdsClient> mds_client_;
  TimerUPtr timer_;
  RemoteNodeGorupSPtr group_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_NODE_MANAGER_H_
