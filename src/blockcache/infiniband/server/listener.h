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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_SERVER_LISTENER_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_SERVER_LISTENER_H_

#include <memory>

#include "blockcache/common/status.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/infiniband/common/protocol.h"
#include "blockcache/infiniband/connection/connection.h"
#include "blockcache/net/controller.h"
#include "blockcache/net/service.h"
#include "dingofs/cache.pb.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

class Listener {
 public:
  Listener(Device* device, BufferPool* buffer_pool,
           CompletionQueue* completion_queue);

  Listener(const Listener&) = delete;
  Listener& operator=(const Listener&) = delete;

  StatusOr<ConnectionUPtr> Accept(const HandshakeMsg& peer,
                                  HandshakeMsg* local);

 private:
  Status CheckPeerInfo(const HandshakeMsg& peer) const;
  void GetLocalInfo(const QueuePairGroup& qps, HandshakeMsg* msg) const;

  Device* device_;
  BufferPool* buffer_pool_;
  CompletionQueue* completion_queue_;
};

using ListenerUPtr = std::unique_ptr<Listener>;

class SessionManager;

class HandshakeHandler {
 public:
  HandshakeHandler(Listener* listener, SessionManager* session_manager,
                   ServiceRegistry* service_registry);

  HandshakeHandler(const HandshakeHandler&) = delete;
  HandshakeHandler& operator=(const HandshakeHandler&) = delete;

  Future<> Handshake(Controller* cntl,
                     const pb::blockcache::HandshakeRequest* request,
                     pb::blockcache::HandshakeResponse* response);

 private:
  Future<Status> Establish(HandshakeMsg peer, HandshakeMsg* local);

  Listener* listener_;
  SessionManager* session_manager_;
  ServiceRegistry* service_registry_;
};

using HandshakeHandlerUPtr = std::unique_ptr<HandshakeHandler>;

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif
