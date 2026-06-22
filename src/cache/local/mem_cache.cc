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

/*
 * Project: DingoFS
 * Created Date: 2026-05-28
 * Author: Jingli Chen (Wine93)
 */

#include "cache/local/mem_cache.h"

#include <butil/macros.h>
#include <glog/logging.h>

#include <utility>

#include "common/status.h"
#include "utils/uuid.h"

namespace dingofs {
namespace cache {

MemCache::MemCache(MemCacheOption option)
    : running_(false),
      option_(option),
      shard_capacity_bytes_((option.cache_size_mb * kMiB + kShardCount - 1) /
                            kShardCount),
      uuid_(utils::GenerateUUID()),
      vars_(std::make_unique<MemCacheMetrics>(option.cache_size_mb)) {}

Status MemCache::Start(UploadFunc uploader) {
  CHECK_NOTNULL(uploader);

  if (running_.load(std::memory_order_relaxed)) {
    return Status::OK();
  }

  LOG(INFO) << "Memory cache (capacity=" << option_.cache_size_mb
            << "MB, shards=" << kShardCount << ") is starting...";

  uploader_ = std::move(uploader);
  vars_->Reset();
  vars_->running_status.set_value("up");

  running_.store(true, std::memory_order_relaxed);
  LOG(INFO) << "Memory cache is up.";
  return Status::OK();
}

Status MemCache::Shutdown() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }

  LOG(INFO) << "Memory cache is shutting down...";

  vars_->running_status.set_value("down");
  for (auto& shard : shards_) {
    BAIDU_SCOPED_LOCK(shard.mutex);
    if (shard.used_bytes != 0) {
      vars_->used_bytes << -static_cast<int64_t>(shard.used_bytes);
    }
    shard.lru.clear();
    shard.index.clear();
    shard.used_bytes = 0;
  }

  LOG(INFO) << "Memory cache is down.";
  return Status::OK();
}

Status MemCache::Stage(BlockHandle handle, IOBuffer block, StageOption option) {
  if (!IsRunning()) {
    vars_->stage_skips << 1;
    return Status::CacheDown("memory cache is down");
  }

  auto key = handle.Filename();
  size_t length = block.Size();
  Insert(GetShard(key), key, std::move(block));
  uploader_(handle, length, option.block_attr);
  return Status::OK();
}

Status MemCache::RemoveStage(BlockHandle /*handle*/,
                             RemoveStageOption /*option*/) {
  // No-op: staged block stays in memory as a cached block. This mirrors the
  // disk cache behavior where the stage file is hard-linked to the cache file,
  // so removing the stage entry leaves the cache copy intact.
  return Status::OK();
}

Status MemCache::Cache(BlockHandle handle, IOBuffer block,
                       CacheOption /*option*/) {
  if (!IsRunning()) {
    return Status::CacheDown("memory cache is down");
  }

  auto key = handle.Filename();
  Insert(GetShard(key), key, std::move(block));
  return Status::OK();
}

Status MemCache::Load(BlockHandle handle, off_t offset, size_t length,
                      IOBuffer* buffer, LoadOption /*option*/) {
  if (!IsRunning()) {
    vars_->cache_misses << 1;
    return Status::CacheDown("memory cache is down");
  }

  auto key = handle.Filename();
  auto& shard = GetShard(key);

  // Hold the shard lock only long enough to look up, promote in the LRU and
  // grab a refcounted copy of the IOBuffer. The actual data copy into the
  // caller's destination happens outside the lock.
  IOBuffer copy;
  {
    BAIDU_SCOPED_LOCK(shard.mutex);
    auto it = shard.index.find(key);
    if (it == shard.index.end()) {
      vars_->cache_misses << 1;
      return Status::NotFound("cache not found");
    }
    shard.lru.splice(shard.lru.begin(), shard.lru, it->second);
    copy = it->second->data;
  }

  size_t total = copy.Size();
  if (offset < 0 || static_cast<size_t>(offset) > total) {
    vars_->cache_misses << 1;
    return Status::NotFound("cache offset out of range");
  }
  size_t avail = total - static_cast<size_t>(offset);
  size_t want = std::min(length, avail);
  copy.AppendTo(buffer, want, static_cast<size_t>(offset));

  vars_->cache_hits << 1;
  return Status::OK();
}

bool MemCache::IsCached(const BlockHandle& handle) const {
  auto key = handle.Filename();
  const auto& shard = GetShard(key);
  BAIDU_SCOPED_LOCK(shard.mutex);
  return shard.index.find(key) != shard.index.end();
}

bool MemCache::IsFull(const BlockHandle& handle) const {
  const auto& shard = GetShard(handle.Filename());
  BAIDU_SCOPED_LOCK(shard.mutex);
  return shard.used_bytes >= shard_capacity_bytes_;
}

bool MemCache::Dump(Json::Value& value) const {
  uint64_t total_used = 0;
  uint64_t total_count = 0;
  for (const auto& shard : shards_) {
    BAIDU_SCOPED_LOCK(shard.mutex);
    total_used += shard.used_bytes;
    total_count += shard.index.size();
  }
  value["capacity_mb"] = static_cast<Json::UInt64>(option_.cache_size_mb);
  value["used_bytes"] = static_cast<Json::UInt64>(total_used);
  value["block_count"] = static_cast<Json::UInt64>(total_count);
  value["shard_count"] = static_cast<Json::UInt64>(kShardCount);
  return true;
}

void MemCache::Insert(Shard& shard, const std::string& key, IOBuffer block) {
  size_t size = block.Size();
  BAIDU_SCOPED_LOCK(shard.mutex);

  auto it = shard.index.find(key);
  if (it != shard.index.end()) {
    int64_t delta = static_cast<int64_t>(size) -
                    static_cast<int64_t>(it->second->data.Size());
    it->second->data = std::move(block);
    shard.used_bytes =
        static_cast<uint64_t>(static_cast<int64_t>(shard.used_bytes) + delta);
    shard.lru.splice(shard.lru.begin(), shard.lru, it->second);
    vars_->used_bytes << delta;
    return;
  }

  EvictLocked(shard, size);
  shard.lru.push_front({key, std::move(block)});
  shard.index[key] = shard.lru.begin();
  shard.used_bytes += size;
  vars_->used_bytes << static_cast<int64_t>(size);
}

void MemCache::EvictLocked(Shard& shard, uint64_t need_bytes) {
  while (shard.used_bytes + need_bytes > shard_capacity_bytes_ &&
         !shard.lru.empty()) {
    auto& victim = shard.lru.back();
    uint64_t victim_size = victim.data.Size();
    shard.used_bytes -= victim_size;
    vars_->used_bytes << -static_cast<int64_t>(victim_size);
    shard.index.erase(victim.key);
    shard.lru.pop_back();
  }
}

}  // namespace cache
}  // namespace dingofs
