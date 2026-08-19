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

#ifndef DINGOFS_BLOCKCACHE_NET_BRPC_TRANSPORT_H_
#define DINGOFS_BLOCKCACHE_NET_BRPC_TRANSPORT_H_

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "blockcache/net/transport.h"
#include "common/status.h"

namespace brpc {
class Server;
}

namespace dingofs {
namespace blockcache {

class TransportServiceImpl;

// Spins out calls in flight; completions land on bthreads, nowhere to park.
inline void DrainInflight(const std::atomic<int64_t>& inflight) {
  while (inflight.load(std::memory_order_acquire) > 0) {
    std::this_thread::yield();
  }
}

// Serves a shard's RequestHandler over brpc; the shard runs `done` itself.
class BrpcTransport final : public ServerTransport {
 public:
  struct Option {
    std::string listen_ip = "0.0.0.0";
    uint16_t listen_port = 0;  // 0 lets the kernel choose
    int idle_timeout_sec = -1;
    // brpc's admission control: the only bound on requests in flight.
    int max_concurrency = 0;
    // Run brpc's `done` on a bthread instead of on the shard.
    bool reply_on_bthread = false;
    // Routing is not an option here: the shard-picking rule is server-wide.
  };

  // Out of line: TransportServiceImpl is completed only in the .cc.
  explicit BrpcTransport(Option option);
  ~BrpcTransport() override;

  Status Start(const ServerContext& context) override;

  // Drains requests already handed to a shard; returns before runtime stops.
  void Shutdown() override;

  // In-flight accounting, kept by the request path.
  void CallStarted() { inflight_.fetch_add(1, std::memory_order_relaxed); }
  void CallFinished() { inflight_.fetch_sub(1, std::memory_order_release); }

  const char* name() const override { return "brpc"; }
  const ServerContext& context() const { return context_; }
  bool reply_on_bthread() const { return option_.reply_on_bthread; }

 private:
  Option option_;
  ServerContext context_;

  std::unique_ptr<::brpc::Server> server_;
  std::unique_ptr<TransportServiceImpl> impl_;
  bool started_ = false;
  std::atomic<int64_t> inflight_{0};
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_NET_BRPC_TRANSPORT_H_
