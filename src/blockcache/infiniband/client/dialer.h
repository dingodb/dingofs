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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_CLIENT_DIALER_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_CLIENT_DIALER_H_

#include <functional>
#include <memory>

#include "blockcache/common/status.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/core/runtime/shard_inbox.h"
#include "blockcache/infiniband/base/buffer_pool.h"
#include "blockcache/infiniband/base/completion_queue.h"
#include "blockcache/infiniband/base/device.h"
#include "blockcache/infiniband/common/protocol.h"
#include "blockcache/infiniband/connection/connection.h"
#include "blockcache/infiniband/connection/queue_pairs.h"
#include "blockcache/net/channel.h"
#include "dingofs/cache.pb.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

class Handshaker : public InboxWork {
 public:
  static Future<Status> Handshake(
      const ChannelOption& option,
      const pb::blockcache::HandshakeRequest& request,
      pb::blockcache::HandshakeResponse* response);

 private:
  Handshaker(unsigned shard, ChannelOption option,
             const pb::blockcache::HandshakeRequest& request,
             pb::blockcache::HandshakeResponse* response);

  static void OnWorker(InboxWork* base);
  static void* OnBthread(void* arg);
  static void OnShard(InboxWork* base);

  Status Call();

  const unsigned shard_;
  const ChannelOption option_;
  const pb::blockcache::HandshakeRequest request_;
  pb::blockcache::HandshakeResponse* const response_;
  Status status_;
  Promise<Status> promise_;
};

class Dialer {
 public:
  Dialer(Device* device, BufferPool* buffer_pool,
         CompletionQueue* completion_queue);
  ~Dialer();

  Dialer(const Dialer&) = delete;
  Dialer& operator=(const Dialer&) = delete;

  Future<StatusOr<ConnectionUPtr>> Dial(ChannelOption option);

 private:
  void GetLocalInfo(const QueuePairGroup& qps, HandshakeMsg* local) const;
  Status GetPeerInfo(const pb::blockcache::HandshakeResponse& response,
                     HandshakeMsg* peer) const;
  Status ReserveCapacity();

  Device* device_;
  BufferPool* buffer_pool_;
  CompletionQueue* completion_queue_;
  std::function<void()> on_destroy_;
};

using DialerUPtr = std::unique_ptr<Dialer>;

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif
