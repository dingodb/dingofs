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

#ifndef DINGOFS_SRC_CACHE_CACHEGROUP_SERVER_H_
#define DINGOFS_SRC_CACHE_CACHEGROUP_SERVER_H_

#include <brpc/server.h>

#include <csignal>

#include "cache/cachegroup/node.h"
#include "dingofs/blockcache.pb.h"

namespace dingofs {
namespace cache {

class Server {
 public:
  Server();
  Status Start();
  Status Shutdown();

 private:
  void InstallSignal();
  Status StartRpcServer(const std::string& listen_ip, uint32_t listen_port);

  std::atomic<bool> running_;
  CacheNodeSPtr node_;
  std::unique_ptr<pb::cache::BlockCacheService> service_;
  std::unique_ptr<brpc::Server> server_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_SERVER_H_
