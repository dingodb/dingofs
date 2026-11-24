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

#ifndef DINGOFS_BLOCKCACHE_COMMON_ROUTE_H_
#define DINGOFS_BLOCKCACHE_COMMON_ROUTE_H_

#include <cstdint>

#include "blockcache/common/block_handle.h"
#include "blockcache/core/runtime/smp.h"
#include "blockcache/utils/hash.h"

namespace dingofs {
namespace blockcache {

inline uint64_t GetRouteKey(BlockHandle handle) { return HashInt(handle.id); }

inline unsigned GetShard(uint64_t route_key, unsigned shards) {
  return static_cast<unsigned>((static_cast<__uint128_t>(route_key) * shards) >>
                               64);
}

inline unsigned GetOwnerShard(BlockHandle handle, unsigned shards) {
  return GetShard(GetRouteKey(handle), shards);
}

inline unsigned GetOwnerShard(BlockHandle handle) {
  return GetOwnerShard(handle, ShardCount());
}

// The smallest route key that GetShard() maps to `shard`.
inline uint64_t GetShardStartKey(unsigned shard, unsigned shards) {
  if (shard == 0 || shards <= 1) {
    return 0;
  }

  // ceil(shard * 2^64 / shards)
  const __uint128_t scaled =
      (static_cast<__uint128_t>(shard) << 64) + shards - 1;
  return static_cast<uint64_t>(scaled / shards);
}

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_COMMON_ROUTE_H_
