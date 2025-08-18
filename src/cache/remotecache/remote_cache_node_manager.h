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

#include "cache/common/mds_client.h"
#include "common/status.h"
#include "utils/executor/executor.h"

namespace dingofs {
namespace cache {

using OnMemberLoadFn =
    std::function<void(const std::vector<CacheGroupMember>& members)>;

class RemoteCacheNodeManager {
 public:
  explicit RemoteCacheNodeManager(OnMemberLoadFn on_member_load_fn);

  Status Start();
  void Shutdown();

 private:
  void BackgroudRefresh();
  Status RefreshMembers();
  Status LoadMembers(std::vector<CacheGroupMember>* members);

  void SetStatusPage() const;

  std::atomic<bool> running_;
  MDSClientUPtr mds_client_;
  OnMemberLoadFn on_member_load_fn_;
  std::unique_ptr<Executor> executor_;
};

using RemoteCacheNodeManagerUPtr = std::unique_ptr<RemoteCacheNodeManager>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_CACHE_NODE_MANAGER_H_
