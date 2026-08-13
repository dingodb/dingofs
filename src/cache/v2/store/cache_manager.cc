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

#include "cache/v2/store/cache_manager.h"

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/vfs.h>

#include <cerrno>
#include <cstring>
#include <utility>

#include "cache/v2/core/fs/filesystem.h"
#include "cache/v2/core/runtime/smp.h"
#include "cache/v2/store/local_filesystem.h"
#include "cache/v2/utils/align.h"
#include "cache/v2/utils/time.h"

namespace dingofs {
namespace cache {
namespace v2 {

DEFINE_uint32(cache_expire_s, 259200,
              "expire time of cached blocks (0 means never)");
DEFINE_uint32(cache_cleanup_expire_interval_ms, 1000,
              "interval to evict expired blocks");
DEFINE_double(free_space_ratio, 0.1, "min free space (ratio)");

static uint64_t ShardShare(uint64_t total) { return total / ShardCount(); }

CacheManager::CacheManager(const DiskCacheLayout& layout, uint64_t capacity)
    : layout_(layout), capacity_(capacity), policy_(NewEvictionPolicy()) {}

Future<> CacheManager::Start() {
  LOG(INFO) << "CacheManager(" << layout_.RootDir() << ") is starting...";

  running_ = true;
  Future<> workers = WhenAll(CheckFreeSpace(), CleanupExpire(), DeleteBlocks());

  LOG(INFO) << "Successfully start CacheManager{dir=" << layout_.RootDir()
            << " capacity=" << (capacity_ >> 20)
            << "mb cache_expire_s=" << FLAGS_cache_expire_s
            << " cache_cleanup_expire_interval_ms="
            << FLAGS_cache_cleanup_expire_interval_ms
            << " free_space_ratio=" << FLAGS_free_space_ratio << "}";
  return workers;
}

Future<> CacheManager::Shutdown() {
  LOG(INFO) << "CacheManager{dir=" << layout_.RootDir()
            << "} is shutting down...";

  running_ = false;
  co_await gate_.Close();

  LOG(INFO) << "Successfully shutdown CacheManager{dir=" << layout_.RootDir()
            << "}";
}

void CacheManager::Insert(const BlockHandle& handle, uint32_t atime,
                          bool staged) {
  auto [it, inserted] = entries_.try_emplace(handle);
  if (!inserted) {
    EraseEntry(it);
    it = entries_.try_emplace(handle).first;
  }

  CacheEntry& e = it->second;
  e.key = &it->first;
  e.size = handle.size;
  e.atime = atime;
  e.staged = staged;
  used_bytes_ += AlignUp<uint64_t>(handle.size, kBlockAlign);
  if (staged) {
    staged_blocks_++;
  } else {
    policy_->OnInsert(&e);
  }

  if (used_bytes_ >= capacity_) {
    CleanupFull(used_bytes_ - (capacity_ / 100 * 95), 0);
  }
}

std::optional<CacheManager::Entry> CacheManager::Find(
    const BlockHandle& handle) const {
  const auto it = entries_.find(handle);
  if (it == entries_.end()) {
    return std::nullopt;
  }

  return Entry{.handle = it->first, .staged = it->second.staged};
}

std::optional<CacheManager::Entry> CacheManager::Touch(
    const BlockHandle& handle) {
  auto it = entries_.find(handle);
  if (it == entries_.end()) {
    return std::nullopt;
  }

  CacheEntry& e = it->second;
  e.atime = TimestampSec();
  if (!e.staged) {
    policy_->OnAccess(&e);
  }
  return Entry{.handle = it->first, .staged = e.staged};
}

void CacheManager::EraseCache(const BlockHandle& handle) {
  auto it = entries_.find(handle);
  if (it != entries_.end()) {
    EraseEntry(it);
  }
}

void CacheManager::EraseStage(const BlockHandle& handle) {
  auto it = entries_.find(handle);
  if (it != entries_.end() && it->second.staged) {
    it->second.staged = false;
    staged_blocks_--;
    policy_->OnInsert(&it->second);
  }
}

void CacheManager::EraseEntry(BlockIndex::iterator it) {
  CacheEntry& e = it->second;
  if (e.staged) {
    staged_blocks_--;
  } else {
    policy_->OnErase(&e);
  }

  used_bytes_ -= AlignUp<uint64_t>(e.size, kBlockAlign);
  entries_.erase(it);
}

void CacheManager::CleanupFull(uint64_t want_free_bytes,
                               uint64_t want_free_files) {
  Evicted to_del;
  policy_->Evict(want_free_bytes, want_free_files, &to_del);
  EnterDeleteQueue(to_del, false);
}

Future<> CacheManager::CheckFreeSpace() {
  Gate::Holder holder(gate_);
  CHECK(holder.ok());

  while (running_) {
    co_await SleepWhile([this] { return running_; }, 1000);
    if (!running_) {
      break;
    }

    struct statfs sfs{};
    if (::statfs(layout_.RootDir().c_str(), &sfs) != 0) {
      LOG(ERROR) << "Fail to statfs, dir=`" << layout_.RootDir()
                 << "': " << std::strerror(errno);
      continue;
    } else if (sfs.f_blocks == 0 || sfs.f_files == 0) {
      LOG(WARNING) << "Skip free space check, dir=`" << layout_.RootDir()
                   << "': f_blocks=" << sfs.f_blocks
                   << " f_files=" << sfs.f_files;
      continue;
    }

    const double ratio = FLAGS_free_space_ratio;
    const double free_bytes = static_cast<double>(sfs.f_bavail) / sfs.f_blocks;
    const double free_files = static_cast<double>(sfs.f_ffree) / sfs.f_files;
    cache_full_ = free_bytes < ratio || free_files < ratio;
    stage_full_ = free_bytes < ratio / 2 || free_files < ratio / 2;
    if (!cache_full_) {
      continue;
    }

    uint64_t want_free_bytes = 0;
    uint64_t want_free_files = 0;
    if (free_bytes < ratio) {
      want_free_bytes = static_cast<uint64_t>((ratio - free_bytes) *
                                              sfs.f_blocks * sfs.f_bsize);
    }
    if (free_files < ratio) {
      want_free_files =
          static_cast<uint64_t>((ratio - free_files) * sfs.f_files);
    }
    if (want_free_bytes > 0 || want_free_files > 0) {
      CleanupFull(ShardShare(want_free_bytes) + 1,
                  want_free_files > 0 ? ShardShare(want_free_files) + 1 : 0);
    }
  }
}

Future<> CacheManager::CleanupExpire() {
  Gate::Holder holder(gate_);
  CHECK(holder.ok());
  const uint64_t budget = ShardShare(1000) + 1;
  while (running_) {
    co_await SleepWhile([this] { return running_; },
                        FLAGS_cache_cleanup_expire_interval_ms);
    if (!running_ || FLAGS_cache_expire_s == 0) {
      continue;
    }

    Evicted to_del;
    policy_->EvictExpired(TimestampSec(), FLAGS_cache_expire_s, budget,
                          &to_del);
    EnterDeleteQueue(to_del, true);
  }
}

void CacheManager::EnterDeleteQueue(const Evicted& to_del, bool expired) {
  if (to_del.empty()) {
    return;
  }

  DeleteBatch batch{.reason = expired ? "expired" : "cache full"};
  batch.items.reserve(to_del.size());
  for (CacheEntry* victim : to_del) {
    const BlockHandle handle = *victim->key;
    if (expired) {
      expired_blocks_++;
    } else {
      evicted_blocks_++;
      evicted_bytes_ += victim->size;
    }

    used_bytes_ -= AlignUp<uint64_t>(victim->size, kBlockAlign);
    batch.items.push_back(
        DelItem{.path = layout_.CachePath(handle), .size = victim->size});
    entries_.erase(handle);
  }
  unlink_queue_.push_back(std::move(batch));
}

Future<> CacheManager::DeleteBlocks() {
  Gate::Holder holder(gate_);
  CHECK(holder.ok());
  while (running_) {
    if (unlink_queue_.empty()) {
      co_await SleepWhile([this] { return running_ && unlink_queue_.empty(); },
                          1000);
      continue;
    }

    const DeleteBatch batch = std::move(unlink_queue_.front());
    unlink_queue_.pop_front();

    const uint64_t began = TimestampNs();
    uint64_t deleted = 0;
    uint64_t freed_bytes = 0;
    for (const DelItem& item : batch.items) {
      const Status status = co_await FileSystem::Unlink(item.path);
      if (!status.ok()) {
        LOG_IF(WARNING, !status.IsNotExist())
            << "Fail to unlink evicted block " << item.path << ": "
            << status.ToString();
        continue;
      }
      deleted++;
      freed_bytes += item.size;
    }
    LOG(INFO) << fmt::format(
        "{} cache blocks deleted for {}, free {:.2f} MiB, costs {:.6f} "
        "seconds.",
        deleted, batch.reason, freed_bytes / 1048576.0,
        (TimestampNs() - began) / 1e9);
  }
}

CacheStats CacheManager::GetStats() const {
  CacheStats stats;
  stats.capacity_bytes = capacity_;
  stats.used_bytes = used_bytes_;
  stats.staged_blocks = staged_blocks_;
  stats.cached_blocks = entries_.size() - staged_blocks_;
  stats.evicted_blocks = evicted_blocks_;
  stats.evicted_bytes = evicted_bytes_;
  stats.expired_blocks = expired_blocks_;
  for (const DeleteBatch& batch : unlink_queue_) {
    stats.pending_unlinks += batch.items.size();
  }
  return stats;
}

}  // namespace v2
}  // namespace cache
}  // namespace dingofs
