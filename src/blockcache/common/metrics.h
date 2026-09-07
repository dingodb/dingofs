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

#ifndef DINGOFS_BLOCKCACHE_COMMON_METRICS_H_
#define DINGOFS_BLOCKCACHE_COMMON_METRICS_H_

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

#include "blockcache/common/stats.h"

namespace dingofs {
namespace blockcache {

struct CacheGroupMember;
using Members = std::vector<CacheGroupMember>;

class Counter {
 public:
  Counter& operator+=(uint64_t n) { return *this = Get() + n; }
  Counter& operator=(uint64_t value) {
    value_.store(value, std::memory_order_relaxed);
    return *this;
  }
  uint64_t Get() const { return value_.load(std::memory_order_relaxed); }

 private:
  std::atomic<uint64_t> value_{0};
};

struct LocalCacheVars {
  Counter load_bytes;
  Counter stage_bytes;
  Counter cache_bytes;
};

struct RemoteCacheVars {
  Counter range_bytes;
  Counter put_bytes;
  Counter cache_bytes;
  Counter hits;
  Counter misses;
  const Members* members = nullptr;  // registered by the shard's node group
};

struct DiskCacheVars {
  uint32_t index = 0;
  std::string dir;
  std::string uuid;
  Counter capacity_bytes;
  Counter used_bytes;
  Counter cached_blocks;
  Counter staged_blocks;
  Counter hits;
  Counter misses;
  Counter health;
  Counter stage_full;
  Counter cache_full;
  Counter running;
};

inline thread_local LocalCacheVars tls_local_cache_vars;
inline thread_local RemoteCacheVars tls_remote_cache_vars;

inline LocalCacheVars& ThisLocalCacheVars() { return tls_local_cache_vars; }
inline RemoteCacheVars& ThisRemoteCacheVars() { return tls_remote_cache_vars; }

void RegisterDiskCacheVars(DiskCacheVars* vars);
void UnregisterDiskCacheVars(DiskCacheVars* vars);

void ExposeMetrics();
void HideMetrics();

std::vector<DiskStats> SnapshotDisks();
Members SnapshotMembers();

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_COMMON_METRICS_H_
