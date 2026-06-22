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

#ifndef DINGOFS_SRC_CACHE_NODE_CACHE_SERVER_H_
#define DINGOFS_SRC_CACHE_NODE_CACHE_SERVER_H_

#include <brpc/server.h>

#include <csignal>
#include <memory>
#include <vector>

#include "cache/node/node.h"
#include "cache/infiniband/memory.h"
#include "cache/infiniband/server.h"
#include "dingofs/blockcache.pb.h"
#include "utils/logclean_manager.h"

namespace dingofs {
namespace cache {

class CacheServer {
 public:
  CacheServer();
  Status Start();
  Status Shutdown();

 private:
  void InstallSignal();
  Status StartRDMAServer();
  Status StartBrpcServer(const std::string& listen_ip, uint32_t listen_port);
  void ShutdownRDMAServer() { rdma_server_->Shutdown(); }
  void ShutdownBrpcServer() { brpc::AskToQuit(); }

  std::atomic<bool> running_;
  std::unique_ptr<CacheNode> node_;
  std::unique_ptr<pb::cache::BlockCacheService> rdma_service_;
  std::unique_ptr<pb::cache::BlockCacheService> brpc_service_;
  std::unique_ptr<infiniband::Server> rdma_server_;
  std::unique_ptr<brpc::Server> brpc_server_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_NODE_CACHE_SERVER_H_
