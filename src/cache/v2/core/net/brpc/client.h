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

#ifndef DINGOFS_CACHE_V2_CORE_NET_BRPC_CLIENT_H_
#define DINGOFS_CACHE_V2_CORE_NET_BRPC_CLIENT_H_

#include <atomic>
#include <memory>
#include <string>

#include "cache/v2/common/status.h"
#include "cache/v2/core/net/client.h"

namespace brpc {
class Channel;
}

namespace dingofs {
namespace cache {
namespace v2 {

// The caller's side of the brpc transport, owned by one shard.
// Completions hop back through the shard's foreign queue to resolve there.
class BrpcClient final : public Client {
 public:
  struct Option {
    std::string server;  // "ip:port"
    int timeout_ms = 1000;
    int connect_timeout_ms = 500;
    int max_retry = 0;
    // brpc pools sockets by (address, group); per-shard group = own socket.
    std::string connection_group;
  };

  static StatusOr<std::unique_ptr<BrpcClient>> Create(unsigned shard,
                                                      const Option& option);

  ~BrpcClient() override;

  BrpcClient(const BrpcClient&) = delete;
  BrpcClient& operator=(const BrpcClient&) = delete;

  // Waits for calls already on the wire; must return before the runtime stops.
  void Shutdown();

  // Send is zero copy; Recv costs one memcpy out of brpc's blocks.
  Future<StatusOr<Reply>> Call(Opcode opcode, std::string_view payload,
                               Body body, uint64_t route_hint) override;

  // The override above would otherwise hide the base's short forms.
  using Client::Call;

  // In-flight accounting, kept by the request path.
  void CallStarted() { inflight_.fetch_add(1, std::memory_order_relaxed); }
  void CallFinished() { inflight_.fetch_sub(1, std::memory_order_release); }

  unsigned shard() const { return shard_; }

 private:
  BrpcClient(unsigned shard, const Option& option);

  unsigned shard_;
  Option option_;
  std::unique_ptr<::brpc::Channel> channel_;
  std::atomic<int64_t> inflight_{0};
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_NET_BRPC_CLIENT_H_
