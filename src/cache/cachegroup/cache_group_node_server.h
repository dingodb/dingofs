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

#ifndef DINGOFS_SRC_CACHE_CACHEGROUP_CACHE_GROUP_NODE_SERVER_H_
#define DINGOFS_SRC_CACHE_CACHEGROUP_CACHE_GROUP_NODE_SERVER_H_

#include <brpc/server.h>

#include <csignal>

#include "cache/cachegroup/cache_group_node.h"

namespace dingofs {
namespace cache {

class CacheGroupNodeServer {
 public:
  virtual ~CacheGroupNodeServer() = default;

  virtual Status Start() = 0;
  virtual Status Shutdown() = 0;
};

class CacheGroupNodeServerImpl final : public CacheGroupNodeServer {
 public:
  explicit CacheGroupNodeServerImpl(CacheGroupNodeOption option);

  ~CacheGroupNodeServerImpl() override = default;

  Status Start() override;
  Status Shutdown() override;

 private:
  void InstallSignal();
  Status StartRpcServer(const std::string& listen_ip, uint32_t listen_port);

  std::atomic<bool> running_;
  const CacheGroupNodeOption option_;
  CacheGroupNodeSPtr node_;
  std::unique_ptr<PBBlockCacheService> service_;
  std::unique_ptr<brpc::Server> server_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_CACHE_GROUP_NODE_SERVER_H_
