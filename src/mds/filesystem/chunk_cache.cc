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

#include "mds/filesystem/chunk_cache.h"

#include <string>
#include <utility>
#include <vector>

#include "utils/time.h"

namespace dingofs {
namespace mds {

static const std::string kChunkCacheCountMetricsName = "dingofs_{}_chunk_cache_count";

ChunkCache::ChunkCache(uint32_t fs_id)
    : fs_id_(fs_id), count_metrics_(fmt::format(kChunkCacheCountMetricsName, fs_id)) {}

static ChunkCache::ChunkSPtr NewChunk(ChunkEntry&& chunk) { return std::make_shared<ChunkEntry>(std::move(chunk)); }

bool ChunkCache::PutIf(uint64_t ino, ChunkEntry chunk) {
  bool ret = true;
  chunk_map_.withWLock(
      [this, ino, &ret, &chunk](Map& map) mutable {
        Key key{.ino = ino, .chunk_index = chunk.index()};
        auto it = map.find(key);
        if (it == map.end()) {
          map.insert(std::make_pair(key, NewChunk(std::move(chunk))));

          count_metrics_ << 1;

        } else {
          const auto& old_chunk = it->second;
          if (chunk.version() > old_chunk->version()) {
            it->second = NewChunk(std::move(chunk));
          } else {
            ret = false;
          }
        }
      },
      ino);

  return ret;
}

void ChunkCache::Delete(uint64_t ino, uint64_t chunk_index) {
  chunk_map_.withWLock(
      [this, ino, chunk_index](Map& map) mutable {
        Key key{.ino = ino, .chunk_index = chunk_index};
        auto it = map.find(key);
        if (it == map.end()) return;

        map.erase(it);
        count_metrics_ << -1;
      },
      ino);
}

void ChunkCache::Delete(uint64_t ino) {
  chunk_map_.withWLock(
      [this, ino](Map& map) mutable {
        Key key{.ino = ino, .chunk_index = 0};

        for (auto it = map.lower_bound(key); it != map.end();) {
          if (it->first.ino != ino) break;

          it = map.erase(it);
          count_metrics_ << -1;
        }
      },
      ino);
}

void ChunkCache::BatchDeleteIf(const std::function<bool(const Ino&)>& f) {
  chunk_map_.iterateWLock([&](Map& map) {
    for (auto it = map.begin(); it != map.end();) {
      if (f(it->first.ino)) {
        it = map.erase(it);
        count_metrics_ << -1;

      } else {
        ++it;
      }
    }
  });
}

uint64_t ChunkCache::Size() {
  uint64_t size = 0;
  chunk_map_.iterate([&size](Map& map) { size += map.size(); });

  return size;
}

ChunkCache::ChunkSPtr ChunkCache::Get(uint64_t ino, uint64_t chunk_index) {
  ChunkCache::ChunkSPtr chunk;
  chunk_map_.withRLock(
      [ino, chunk_index, &chunk](Map& map) mutable {
        Key key{.ino = ino, .chunk_index = chunk_index};

        auto it = map.find(key);
        if (it != map.end()) chunk = it->second;
      },
      ino);

  return chunk;
}

std::vector<ChunkCache::ChunkSPtr> ChunkCache::Get(uint64_t ino) {
  std::vector<ChunkSPtr> chunks;

  chunk_map_.withRLock(
      [ino, &chunks](Map& map) mutable {
        Key key{.ino = ino, .chunk_index = 0};

        for (auto it = map.lower_bound(key); it != map.end(); ++it) {
          if (it->first.ino != ino) break;

          chunks.push_back(it->second);
        }
      },
      ino);

  return chunks;
}

void ChunkCache::Clear() {
  chunk_map_.iterateWLock([&](Map& map) { map.clear(); });
  count_metrics_.reset();
}

void ChunkCache::RememberCheckCompact(uint64_t ino, uint64_t chunk_index) {
  utils::WriteLockGuard lk(lock_);

  auto key = Key{.ino = ino, .chunk_index = chunk_index};
  check_compact_memo_[key] = utils::TimestampMs();
}

uint64_t ChunkCache::GetLastCheckCompactTimeMs(uint64_t ino, uint64_t chunk_index) {
  utils::ReadLockGuard lk(lock_);

  auto key = Key{.ino = ino, .chunk_index = chunk_index};
  auto it = check_compact_memo_.find(key);
  return (it != check_compact_memo_.end()) ? it->second : 0;
}

void ChunkCache::DescribeByJson(Json::Value& value) { value["count"] = count_metrics_.get_value(); }

}  // namespace mds
}  // namespace dingofs
