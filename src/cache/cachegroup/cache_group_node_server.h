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
#include <cstdlib>
#include <memory>
#include <string>

#include "cache/cachegroup/cache_group_node.h"
#include "cache/cachegroup/cache_group_node_service.h"
#include "options/cache/app.h"

namespace dingofs {
namespace cache {
namespace cachegroup {

using options::cache::AppOption;

class CacheGroupNodeServer {
 public:
  virtual ~CacheGroupNodeServer() = default;

  virtual Status Init() = 0;

  virtual Status Run() = 0;

  virtual void Shutdown() = 0;
};

class CacheGroupNodeServerImpl : public CacheGroupNodeServer {
 public:
  explicit CacheGroupNodeServerImpl(AppOption option);

  ~CacheGroupNodeServerImpl() override = default;

  Status Init() override;

  Status Run() override;

  void Shutdown() override;

 private:
  void InstallSignal();

  Status InitLogger();

  Status StartRpcServer(const std::string& listen_ip, uint32_t listen_port);

 private:
  AppOption option_;
  std::shared_ptr<CacheGroupNode> node_;
  std::unique_ptr<BlockCacheService> service_;
  std::unique_ptr<::brpc::Server> server_;
};

}  // namespace cachegroup
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_CACHE_GROUP_NODE_SERVER_H_
