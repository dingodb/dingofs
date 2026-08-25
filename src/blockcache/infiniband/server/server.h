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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_SERVER_SERVER_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_SERVER_SERVER_H_

#include <memory>
#include <string>
#include <vector>

#include "blockcache/infiniband/base/buffer_pool.h"
#include "blockcache/infiniband/base/completion_channel.h"
#include "blockcache/infiniband/base/completion_queue.h"
#include "blockcache/infiniband/base/device.h"
#include "blockcache/infiniband/base/memory_registry.h"
#include "blockcache/infiniband/connection/poller.h"
#include "blockcache/infiniband/server/listener.h"
#include "blockcache/infiniband/server/session_manager.h"
#include "blockcache/net/brpc/brpc_server.h"
#include "blockcache/net/service.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

class InfinibandPoller;

struct ShardContext {
  ShardContext() = default;
  ~ShardContext() = default;

  Device* device;
  BufferPoolUPtr buffer_pool;
  MemoryRegistryUPtr memory_registry;
  CompletionChannelUPtr completion_channel;
  CompletionQueueUPtr completion_queue;
  InfinibandPollerUPtr poller;
  ServiceRegistryUPtr service_registry;
  ListenerUPtr listener;
  SessionManagerUPtr session_manager;
  HandshakeHandlerUPtr handshake_handler;
};

using ShardContextUPtr = std::unique_ptr<ShardContext>;

struct ServerOption {
  std::string device_name;
  BrpcServer* brpc_server = nullptr;
};

class Server {
 public:
  explicit Server(ServerOption option);
  ~Server();
  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;

  void AddService(Service* service);

  Status Start();
  void Shutdown();

 private:
  Status StartShards();
  Future<Status> StartShard(unsigned shard);

  void ShutdownShards();
  Future<> ShutdownShard(unsigned shard);

  bool running_ = false;
  ServerOption option_;
  std::vector<Service*> services_;
  std::vector<ShardContextUPtr> shards_;
};

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif
