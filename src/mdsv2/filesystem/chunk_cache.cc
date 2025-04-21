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

#include "mdsv2/filesystem/chunk_cache.h"

#include <string>
#include <utility>
#include <vector>

namespace dingofs {
namespace mdsv2 {

static const std::string kChunkCacheCountMetricsName = "dingofs_chunk_cache_count";

ChunkCache::ChunkCache() : count_metrics_(kChunkCacheCountMetricsName) {}

bool ChunkCache::PutIf(uint64_t ino, uint64_t chunk_index, const pb::mdsv2::Chunk& chunk) {
  utils::WriteLockGuard lk(lock_);

  auto key = Key{.ino = ino, .chunk_index = chunk_index};

  auto it = chunk_map_.find(key);
  if (it == chunk_map_.end()) {
    chunk_map_.insert(std::make_pair(key, std::make_shared<pb::mdsv2::Chunk>(chunk)));

    count_metrics_ << 1;

  } else {
    const auto& old_chunk = it->second;
    if (chunk.version() <= old_chunk->version()) {
      return false;
    }

    it->second = std::make_shared<pb::mdsv2::Chunk>(chunk);
  }

  return true;
}

bool ChunkCache::PutIf(uint64_t ino, uint64_t chunk_index, pb::mdsv2::Chunk&& chunk) {
  utils::WriteLockGuard lk(lock_);

  auto key = Key{.ino = ino, .chunk_index = chunk_index};

  auto it = chunk_map_.find(key);
  if (it == chunk_map_.end()) {
    chunk_map_.insert(std::make_pair(key, std::make_shared<pb::mdsv2::Chunk>(std::move(chunk))));

    count_metrics_ << 1;

  } else {
    const auto& old_chunk = it->second;
    if (chunk.version() <= old_chunk->version()) {
      return false;
    }

    it->second = std::make_shared<pb::mdsv2::Chunk>(std::move(chunk));
  }

  return true;
}

void ChunkCache::Delete(uint64_t ino, uint64_t chunk_index) {
  utils::WriteLockGuard lk(lock_);

  auto key = Key{.ino = ino, .chunk_index = chunk_index};
  chunk_map_.erase(key);

  count_metrics_ << -1;
}

void ChunkCache::Delete(uint64_t ino) {
  utils::WriteLockGuard lk(lock_);

  auto key = Key{.ino = ino, .chunk_index = 0};
  for (auto it = chunk_map_.upper_bound(key); it != chunk_map_.end();) {
    if (it->first.ino != ino) {
      break;
    }

    it = chunk_map_.erase(it);
    count_metrics_ << -1;
  }
}

ChunkCache::Value ChunkCache::Get(uint64_t ino, uint64_t chunk_index) {
  utils::ReadLockGuard lk(lock_);

  auto key = Key{.ino = ino, .chunk_index = chunk_index};

  auto it = chunk_map_.find(key);
  return (it != chunk_map_.end()) ? it->second : nullptr;
}

std::vector<ChunkCache::Value> ChunkCache::Get(uint64_t ino) {
  utils::ReadLockGuard lk(lock_);

  auto key = Key{.ino = ino, .chunk_index = 0};

  std::vector<ChunkCache::Value> chunks;
  for (auto it = chunk_map_.upper_bound(key); it != chunk_map_.end(); ++it) {
    if (it->first.ino != ino) {
      break;
    }

    chunks.push_back(it->second);
  }

  return chunks;
}

}  // namespace mdsv2
}  // namespace dingofs
