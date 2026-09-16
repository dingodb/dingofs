// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "client/vfs/metasystem/mds/statistics.h"

#include "common/options/client.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

DirAccessStatsSPtr AccessStatsMap::GetOrCreate(Ino ino) {
  // Fast path: hits only read, so concurrent accessors of the same directory
  // (or of the same shard) no longer serialize on a write lock per call.
  DirAccessStatsSPtr stats;
  shard_map_.withRLock(
      [&stats, ino](Map& map) {
        auto it = map.find(ino);
        if (it != map.end()) stats = it->second;
      },
      ino);

  if (stats == nullptr) {
    // Slow path: only the first access to a directory takes the write lock.
    shard_map_.withWLock(
        [this, ino, &stats](Map& map) {
          auto [it, inserted] = map.try_emplace(ino);
          if (inserted) {
            it->second = DirAccessStats::New(ino, watchers_);
            total_count_ << 1;
          }
          stats = it->second;
        },
        ino);

    // Cover the window where the entry exists but no counter bumped it yet.
    stats->UpdateLastActiveTimeS();
  }

  return stats;
}

void AccessStatsMap::CleanExpired(uint64_t expire_s) {
  const uint32_t kShrinkRatio = 32;
  const size_t trigger_size =
      FLAGS_vfs_meta_clean_threshold_count / kShrinkRatio;
  if (Size() < trigger_size) return;

  shard_map_.iterateWLock([&](Map& map) {
    for (auto it = map.begin(); it != map.end();) {
      if (it->second->GetLastActiveTimeS() < expire_s) {
        auto temp = it++;
        map.erase(temp);
        clean_count_ << 1;

      } else {
        ++it;
      }
    }
  });
}

size_t AccessStatsMap::Size() {
  size_t total_size = 0;
  shard_map_.iterate([&](Map& map) { total_size += map.size(); });
  return total_size;
}

size_t AccessStatsMap::Bytes() { return Size() * sizeof(DirAccessStats); }

void AccessStatsMap::Summary(Json::Value& value) {
  value["name"] = "dentrycache";
  value["count"] = Size();
  value["bytes"] = Bytes();
  value["total_count"] = total_count_.get_value();
  value["clean_count"] = clean_count_.get_value();
}

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs