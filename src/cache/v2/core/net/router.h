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

#ifndef DINGOFS_CACHE_V2_CORE_NET_ROUTER_H_
#define DINGOFS_CACHE_V2_CORE_NET_ROUTER_H_

#include <glog/logging.h>

#include <cstdint>

#include "cache/v2/common/route.h"
#include "cache/v2/core/net/types.h"

namespace dingofs {
namespace cache {
namespace v2 {

// Everything routing may look at, available before the request is parsed.
struct RequestRoute {
  Opcode opcode = 0;
  // A key hash the CALLER computed; 0 means it did not supply one.
  uint64_t route_hint = 0;
};

// The one routing rule; runs on the receiving thread, never a shard.
inline unsigned RouteToShard(const RequestRoute& route, unsigned shards) {
  if (route.route_hint != 0) {
    return ShardOf(route.route_hint, shards);
  }
  // Deterministic fallback so a hintless verb lands somewhere reproducible.
  LOG_EVERY_N(WARNING, 10000)
      << "an rpc arrived with no route hint; a verb whose state is sharded by "
         "key will be served by the wrong core";
  return ShardOf(route.opcode, shards);
}

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_NET_ROUTER_H_
