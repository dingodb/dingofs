/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_CACHE_V2_NODE_NODE_H_
#define DINGOFS_CACHE_V2_NODE_NODE_H_

#include "cache/v2/block/sharded.h"
#include "cache/v2/common/mds_client.h"
#include "cache/v2/core/server/server.h"
#include "cache/v2/node/heartbeat.h"
#include "cache/v2/node/membership.h"
#include "cache/v2/node/service.h"

namespace dingofs {
namespace cache {
namespace v2 {

class CacheNode {
 public:
  CacheNode();
  ~CacheNode();

  CacheNode(const CacheNode&) = delete;
  CacheNode& operator=(const CacheNode&) = delete;

  Status Start();
  void Shutdown();
  void RunUntilAskedToQuit();

 private:
  friend class CacheNodeBuilder;
  explicit CacheNode(MDSClientUPtr mds_client);

  Status StartServer();

  bool running_ = false;
  MDSClientUPtr mds_client_;
  ShardedLocalCacheUPtr block_cache_;
  ServerUPtr server_;
  GroupMembershipUPtr membership_;
  HeartbeatUPtr heartbeat_;
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_NODE_NODE_H_
