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

#ifndef DINGOFS_CACHE_V2_STORE_STATS_H_
#define DINGOFS_CACHE_V2_STORE_STATS_H_

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

namespace dingofs {
namespace cache {
namespace v2 {

enum class DiskHealth : uint8_t { kNormal = 0, kUnstable = 1, kDown = 2 };

struct DiskStats {
  std::string uuid;
  std::string dir;
  uint64_t capacity_bytes = 0;
  uint64_t used_bytes = 0;
  DiskHealth health = DiskHealth::kNormal;
  bool stage_full = false;
  bool cache_full = false;
};

struct CacheStats {
  void Merge(const CacheStats& part) {
    capacity_bytes += part.capacity_bytes;
    used_bytes += part.used_bytes;
    cached_blocks += part.cached_blocks;
    staged_blocks += part.staged_blocks;
    hits += part.hits;
    misses += part.misses;
    evicted_blocks += part.evicted_blocks;
    evicted_bytes += part.evicted_bytes;
    expired_blocks += part.expired_blocks;
    pending_unlinks += part.pending_unlinks;
    io_errors += part.io_errors;

    for (const DiskStats& d : part.disks) {
      DiskStats* row = nullptr;

      for (DiskStats& r : disks) {
        if (r.uuid == d.uuid) {
          row = &r;
          break;
        }
      }

      if (row == nullptr) {
        disks.push_back(d);
        continue;
      }

      row->capacity_bytes += d.capacity_bytes;
      row->used_bytes += d.used_bytes;
      row->health = std::max(row->health, d.health);
      row->stage_full |= d.stage_full;
      row->cache_full |= d.cache_full;
    }
  }

  uint64_t capacity_bytes = 0;
  uint64_t used_bytes = 0;
  uint64_t cached_blocks = 0;
  uint64_t staged_blocks = 0;
  uint64_t hits = 0;
  uint64_t misses = 0;
  uint64_t evicted_blocks = 0;
  uint64_t evicted_bytes = 0;
  uint64_t expired_blocks = 0;
  uint64_t pending_unlinks = 0;
  uint64_t io_errors = 0;
  std::vector<DiskStats> disks;
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_STORE_STATS_H_
