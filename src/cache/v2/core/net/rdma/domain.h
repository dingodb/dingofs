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

#ifndef DINGOFS_CACHE_V2_CORE_NET_RDMA_DOMAIN_H_
#define DINGOFS_CACHE_V2_CORE_NET_RDMA_DOMAIN_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "cache/v2/common/status.h"
#include "cache/v2/core/memory/slab_pool.h"
#include "cache/v2/core/net/client.h"
#include "cache/v2/core/net/handler.h"
#include "cache/v2/core/net/rdma/completion_poller.h"
#include "cache/v2/core/net/rdma/connection.h"
#include "cache/v2/core/net/rdma/connection_table.h"
#include "cache/v2/core/net/rdma/memory_registry.h"
#include "cache/v2/core/net/rdma/option.h"
#include "cache/v2/core/net/rdma/send_queue.h"
#include "cache/v2/core/net/rdma/verbs/completion_queue.h"
#include "cache/v2/core/net/rdma/verbs/device.h"
#include "cache/v2/core/reactor/coroutine.h"
#include "cache/v2/core/reactor/timer.h"
#include "cache/v2/utils/gate.h"

namespace dingofs {
namespace cache {
namespace v2 {

// One shard's RDMA resources. No listener: the handshake rides an rpc on
// whatever transport already serves the process, so rdma owns no port.
class RdmaDomain {
 public:
  // `handler` is borrowed and must outlive the domain.
  static StatusOr<std::unique_ptr<RdmaDomain>> Create(const RdmaOption& option,
                                                      RequestHandler* handler);

  ~RdmaDomain();

  RdmaDomain(const RdmaDomain&) = delete;
  RdmaDomain& operator=(const RdmaDomain&) = delete;

  // A range peers may read or write one-sidedly; defaults to the whole pool.
  void Expose(const ExposedRegion& region) { exposed_ = region; }

  // Server transport only: publishes this shard's domain, arms the sweep.
  void StartServing();
  Future<> Shutdown();

  // One rpc over `bootstrap` carries the whole handshake; its timeout is
  // what bounds this call. `route_hint` picks the peer shard.
  Future<StatusOr<RdmaConnection*>> Connect(Client& bootstrap,
                                            uint64_t route_hint);

  // The responder half, run by the handshake verb on the owning shard.
  Future<StatusOr<RdmaConnection*>> Accept(const HandshakeMsg& peer,
                                           HandshakeMsg* mine);

  // Drops one connection and gives its completion budget back; the rest of
  // the domain keeps serving. For an domain shared by many callers.
  Future<> Retire(RdmaConnection* conn);

  // The domain serving this shard; null on a shard that serves no rdma.
  static RdmaDomain* Serving();

  SlabPool& pool() { return pool_; }
  MemoryRegistry& memory() { return memory_; }
  verbs::Device& device() { return *device_; }
  const RdmaOption& option() const { return option_; }
  uint8_t rd_atomic_cap() const { return device_->max_rd_atomic(); }
  size_t connections() const { return conns_.size(); }

 private:
  // Machinery for this domain's own connections and poller, not API.
  friend class RdmaConnection;
  friend class LaneSet;
  friend class Messenger;
  friend class RdmaRequest;
  friend class CompletionPoller;

  RdmaDomain(const RdmaOption& option, RequestHandler* handler);

  Status Init();
  // One sweep pass: mark silent connections dead, retire, unbook their cq.
  Future<> Sweep();
  // The booking is the caller's to give back until the conn is registered.
  Future<StatusOr<std::unique_ptr<RdmaConnection>>> NewConnection();
  Status ApplyHandshake(RdmaConnection* conn, const HandshakeMsg& peer);

  // What one connection books against this shard's completion queue.
  uint32_t ConnectionWrs() const {
    return option_.small_send_wr() + option_.small_recv_wr() +
           (option_.bulk_lanes * option_.bulk_send_wr);
  }

  verbs::CompletionQueue& cq() { return cq_; }
  CompletionPoller& poller() { return *poller_; }
  int comp_channel_fd() const { return comp_channel_.fd(); }
  Gate& gate() { return gate_; }
  // Shared by every connection -- and transport -- serving this shard.
  RequestHandler& handler() { return *handler_; }

  RdmaOption option_;
  verbs::Device* device_ = nullptr;

  // Declaration order is teardown order: queue pairs die before the cq they
  // feed, the cq before its channel.
  verbs::CompletionChannel comp_channel_;
  verbs::CompletionQueue cq_;
  SlabPool pool_;
  MemoryRegistry memory_;
  ExposedRegion exposed_{};
  ConnectionTable conns_;

  // Declared after conns_ so it outlives them: send queues link doorbells
  // into it.
  std::optional<CompletionPoller> poller_;
  Timer sweep_timer_;
  bool stopped_ = false;

  // Handshake/dispatch/sweep coroutines, so Shutdown can wait them out.
  Gate gate_;

  RequestHandler* handler_;
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_NET_RDMA_DOMAIN_H_
