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

#ifndef DINGOFS_CACHE_V2_CORE_NET_TRANSPORT_H_
#define DINGOFS_CACHE_V2_CORE_NET_TRANSPORT_H_

#include <span>

#include "cache/v2/core/net/handler.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace v2 {

// Everything a transport needs from the layer above it, and nothing else.
struct ServerContext {
  // Indexed by shard; filled before any Start, stable until every Shutdown.
  std::span<RequestHandler* const> handlers;

  unsigned shard_count() const {
    return static_cast<unsigned>(handlers.size());
  }
  RequestHandler* HandlerOf(unsigned shard) const { return handlers[shard]; }
};

// A way for requests to reach this process; lifecycle only.
// Start/Shutdown are external-thread only: both block via RunOnAndWait.
class ServerTransport {
 public:
  virtual ~ServerTransport() = default;

  ServerTransport(const ServerTransport&) = delete;
  ServerTransport& operator=(const ServerTransport&) = delete;

  // Begins accepting; everything in `context` outlives the transport.
  virtual Status Start(const ServerContext& context) = 0;

  // Drains in-flight requests; must return before the runtime stops.
  virtual void Shutdown() = 0;

  // For startup and shutdown log lines.
  virtual const char* name() const = 0;

 protected:
  ServerTransport() = default;
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_NET_TRANSPORT_H_
