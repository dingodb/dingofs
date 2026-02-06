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

#include "brpc/reloadable_flags.h"
#include "utils/time.h"

namespace dingofs {
namespace mds {

DEFINE_uint32(mds_chunk_cache_max_count, 4 * 1024 * 1024, "chunk cache max count");
DEFINE_validator(mds_chunk_cache_max_count, brpc::PassValidate);

static const std::string kChunkCacheMetricsPrefix = "dingofs_{}_chunk_cache_{}";

ChunkCache::ChunkCache(uint32_t fs_id)
    : fs_id_(fs_id),
      total_count_(fmt::format(kChunkCacheMetricsPrefix, fs_id, "total_count")),
      access_miss_count_(fmt::format(kChunkCacheMetricsPrefix, fs_id, "miss_count")),
      access_hit_count_(fmt::format(kChunkCacheMetricsPrefix, fs_id, "hit_count")),
      clean_count_(fmt::format(kChunkCacheMetricsPrefix, fs_id, "clean_count")) {}

static ChunkCache::ChunkSPtr NewChunk(ChunkEntry&& chunk) { return std::make_shared<ChunkEntry>(std::move(chunk)); }

bool ChunkCache::PutIf(uint64_t ino, ChunkEntry chunk) {
  chunk.set_expire_time_s(utils::Timestamp());

  bool ret = true;
  shard_map_.withWLock(
      [this, ino, &ret, &chunk](Map& map) mutable {
        Key key{.ino = ino, .chunk_index = chunk.index()};
        auto it = map.find(key);
        if (it == map.end()) {
          map.insert(std::make_pair(key, NewChunk(std::move(chunk))));

          total_count_ << 1;

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
  shard_map_.withWLock(
      [ino, chunk_index](Map& map) mutable {
        Key key{.ino = ino, .chunk_index = chunk_index};
        auto it = map.find(key);
        if (it == map.end()) return;

        map.erase(it);
      },
      ino);
}

void ChunkCache::Delete(uint64_t ino) {
  shard_map_.withWLock(
      [ino](Map& map) mutable {
        Key key{.ino = ino, .chunk_index = 0};

        for (auto it = map.lower_bound(key); it != map.end();) {
          if (it->first.ino != ino) break;

          it = map.erase(it);
        }
      },
      ino);
}

void ChunkCache::BatchDeleteIf(const std::function<bool(const Ino&)>& f) {
  shard_map_.iterateWLock([&](Map& map) {
    for (auto it = map.begin(); it != map.end();) {
      if (f(it->first.ino)) {
        it = map.erase(it);

      } else {
        ++it;
      }
    }
  });
}

ChunkCache::ChunkSPtr ChunkCache::Get(uint64_t ino, uint64_t chunk_index) {
  ChunkCache::ChunkSPtr chunk;
  shard_map_.withRLock(
      [&](Map& map) mutable {
        Key key{.ino = ino, .chunk_index = chunk_index};

        auto it = map.find(key);
        if (it != map.end()) {
          chunk = it->second;
          access_hit_count_ << 1;

        } else {
          access_miss_count_ << 1;
        }
      },
      ino);

  if (chunk != nullptr) chunk->set_expire_time_s(utils::Timestamp());

  return chunk;
}

std::vector<ChunkCache::ChunkSPtr> ChunkCache::Get(uint64_t ino) {
  uint64_t now_s = utils::Timestamp();
  std::vector<ChunkSPtr> chunks;
  shard_map_.withRLock(
      [ino, &chunks, &now_s](Map& map) mutable {
        Key key{.ino = ino, .chunk_index = 0};

        for (auto it = map.lower_bound(key); it != map.end(); ++it) {
          if (it->first.ino != ino) break;

          it->second->set_expire_time_s(now_s);
          chunks.push_back(it->second);
        }
      },
      ino);

  access_hit_count_ << chunks.size();

  return chunks;
}

size_t ChunkCache::Size() {
  size_t size = 0;
  shard_map_.iterate([&size](Map& map) { size += map.size(); });

  return size;
}

size_t ChunkCache::Bytes() {
  size_t bytes = 0;
  shard_map_.iterate([&bytes](Map& map) {
    for (auto& [key, chunk] : map) {
      bytes += sizeof(Key) + sizeof(ChunkEntry) + chunk->slices_size() * sizeof(SliceEntry) +
               chunk->compacted_slices_size() * sizeof(ChunkEntry::CompactedSlice);
    }
  });

  return bytes;
}

void ChunkCache::Clear() {
  shard_map_.iterateWLock([&](Map& map) { map.clear(); });
  total_count_.reset();
  clean_count_.reset();
}

void ChunkCache::CleanExpired(uint64_t expire_s) {
  if (Size() < FLAGS_mds_chunk_cache_max_count) return;

  std::vector<Key> keys;
  shard_map_.iterate([&](const Map& map) {
    for (const auto& [key, chunk] : map) {
      if (chunk->expire_time_s() < expire_s) {
        keys.push_back(key);
      }
    }
  });

  if (keys.empty()) return;

  for (const auto& key : keys) {
    Delete(key.ino, key.chunk_index);
  }

  clean_count_ << keys.size();

  LOG(INFO) << fmt::format("[cache.chunk.{}] clean expired, stat({}|{}|{}).", fs_id_, Size(), keys.size(),
                           clean_count_.get_value());
}

void ChunkCache::DescribeByJson(Json::Value& value) { value["count"] = Size(); }

void ChunkCache::Summary(Json::Value& value) {
  value["name"] = "chunkcache";
  value["count"] = Size();
  value["bytes"] = Bytes();
  value["total_count"] = total_count_.get_value();
  value["clean_count"] = clean_count_.get_value();
  value["hit_count"] = access_hit_count_.get_value();
  value["miss_count"] = access_miss_count_.get_value();
}

}  // namespace mds
}  // namespace dingofs
