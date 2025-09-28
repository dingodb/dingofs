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

namespace dingofs {
namespace mds {

static const std::string kChunkCacheCountMetricsName = "dingofs_{}_chunk_cache_count";

ChunkCache::ChunkCache(uint32_t fs_id)
    : fs_id_(fs_id), count_metrics_(fmt::format(kChunkCacheCountMetricsName, fs_id)) {}

static ChunkCache::ChunkSPtr NewChunk(ChunkEntry&& chunk) { return std::make_shared<ChunkEntry>(std::move(chunk)); }

bool ChunkCache::PutIf(uint64_t ino, ChunkEntry chunk) {
  utils::WriteLockGuard lk(lock_);

  auto key = Key{.ino = ino, .chunk_index = chunk.index()};

  auto it = chunk_map_.find(key);
  if (it == chunk_map_.end()) {
    chunk_map_.insert(std::make_pair(key, NewChunk(std::move(chunk))));

    count_metrics_ << 1;

  } else {
    const auto& old_chunk = it->second;
    if (chunk.version() <= old_chunk->version()) {
      return false;
    }

    it->second = NewChunk(std::move(chunk));
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
  for (auto it = chunk_map_.lower_bound(key); it != chunk_map_.end();) {
    if (it->first.ino != ino) {
      break;
    }

    it = chunk_map_.erase(it);
    count_metrics_ << -1;
  }
}

void ChunkCache::BatchDeleteIf(const std::function<bool(const Ino&)>& f) {
  utils::WriteLockGuard lk(lock_);

  for (auto it = chunk_map_.begin(); it != chunk_map_.end();) {
    if (f(it->first.ino)) {
      it = chunk_map_.erase(it);
      count_metrics_ << -1;

    } else {
      ++it;
    }
  }
}

ChunkCache::ChunkSPtr ChunkCache::Get(uint64_t ino, uint64_t chunk_index) {
  utils::ReadLockGuard lk(lock_);

  auto key = Key{.ino = ino, .chunk_index = chunk_index};

  auto it = chunk_map_.find(key);
  return (it != chunk_map_.end()) ? it->second : nullptr;
}

std::vector<ChunkCache::ChunkSPtr> ChunkCache::Get(uint64_t ino) {
  utils::ReadLockGuard lk(lock_);

  auto key = Key{.ino = ino, .chunk_index = 0};

  std::vector<ChunkSPtr> chunks;
  for (auto it = chunk_map_.lower_bound(key); it != chunk_map_.end(); ++it) {
    if (it->first.ino != ino) {
      break;
    }

    chunks.push_back(it->second);
  }

  return chunks;
}

void ChunkCache::Clear() {
  utils::WriteLockGuard lk(lock_);

  chunk_map_.clear();
  count_metrics_.reset();
}

void ChunkCache::DescribeByJson(Json::Value& value) { value["count"] = count_metrics_.get_value(); }

}  // namespace mds
}  // namespace dingofs
