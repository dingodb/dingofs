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

#ifndef DINGOFS_BLOCKCACHE_NET_SERVER_SERVER_H_
#define DINGOFS_BLOCKCACHE_NET_SERVER_SERVER_H_

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "blockcache/core/runtime/smp.h"
#include "blockcache/net/handler.h"
#include "blockcache/net/rdma/option.h"
#include "blockcache/net/server/service.h"
#include "blockcache/net/server/service_table.h"
#include "blockcache/net/transport.h"
#include "common/status.h"

namespace dingofs {
namespace blockcache {

struct ServerOption {
  // Per-shard registered pool, HCA+io_uring shared; resident: shards x this.
  size_t buffer_pool_bytes = 256u << 20;

  // rdma listens on NO port; its handshake rides another transport (brpc).
  RdmaOption rdma;
};

// The whole server, shaped like brpc's (Start/Shutdown/Run).
//
// Starting the shard threads is the caller's job -- a Server serves on a
// runtime that is already up, and never owns one.
class Server {
 public:
  explicit Server(ServerOption option);
  ~Server();

  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;

  // Constructs one S per shard, ON that shard, from a copy of `args`.
  template <typename S, typename... Args>
  void AddService(Args&&... args) {
    CHECK(!started_) << "AddService after Start()";
    // Runs once per shard, so it copies out of the tuple rather than moving.
    factories_.emplace_back([args = std::make_tuple(std::forward<Args>(
                                 args)...)]() -> std::unique_ptr<Service> {
      return std::apply(
          [](const auto&... a) { return std::make_unique<S>(a...); }, args);
    });
  }

  // Called before Start().
  void AddTransport(std::unique_ptr<ServerTransport> transport);

  Status Start();

  // Drains transports in REVERSE order, THEN drops tables. Idempotent.
  void Shutdown();

  // Parks the calling thread until SIGINT/SIGTERM, then Shutdown() -- brpc's
  // verb of the same name. External thread only, and once per process: the
  // handler and its flag are process-wide, as signals are.
  void RunUntilAskedToQuit();

  // Start(), then RunUntilAskedToQuit().
  Status Run();

 private:
  using Factory = std::function<std::unique_ptr<Service>()>;

  bool RdmaOn() const { return option_.rdma.enabled; }

  Status BuildTables();
  void DropTables();

  ServerOption option_;
  std::vector<Factory> factories_;
  // Slot s is built and destroyed on shard s.
  std::vector<std::unique_ptr<ServiceTable>> tables_;
  std::vector<RequestHandler*> handlers_;
  std::vector<std::unique_ptr<ServerTransport>> transports_;
  // Start()'s own rdma transport, always FIRST: up before and down after
  // every wire that can carry its handshake verb; removed again on Shutdown.
  ServerTransport* own_rdma_ = nullptr;
  bool started_ = false;
};

using ServerUPtr = std::unique_ptr<Server>;

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_NET_SERVER_SERVER_H_
