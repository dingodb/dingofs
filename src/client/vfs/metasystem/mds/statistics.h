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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_MDS_STATISTICS_H_
#define DINGOFS_SRC_CLIENT_VFS_META_MDS_STATISTICS_H_

#include <sys/types.h>

#include <atomic>
#include <memory>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "bvar/bvar.h"
#include "client/vfs/vfs_meta.h"
#include "json/value.h"
#include "utils/shards.h"
#include "utils/time.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

static constexpr uint32_t kWindowSize = 4;

// Lock-free sliding window counter over the last kWindowSize seconds.
// Replaces bvar::WindowEx: no per-instance sampler thread, no heap, no
// thread-local combiner. Each bucket packs (second timestamp << 32 | count).
// ponytail: cross-bucket sums are approximate during concurrent updates; add
// a snapshot lock if exact accounting becomes necessary.
class SlidingWindow {
 public:
  SlidingWindow() {
    for (auto& bucket : buckets_) bucket.store(0, std::memory_order_relaxed);
  }

  // Increments the current second's bucket, returns the window sum.
  uint64_t Inc(uint64_t now_s = utils::SteadyTimestamp()) {
    auto& bucket = buckets_[now_s % kWindowSize];
    uint64_t old = bucket.load(std::memory_order_relaxed);
    while (TimestampOf(old) <= now_s) {
      const uint64_t next = TimestampOf(old) == now_s
                                ? Pack(now_s, CountOf(old) + 1)
                                : Pack(now_s, 1);
      if (bucket.compare_exchange_weak(old, next, std::memory_order_relaxed)) {
        break;
      }
    }

    uint64_t sum = 0;
    const uint64_t oldest = now_s + 1 - kWindowSize;
    for (const auto& b : buckets_) {
      uint64_t v = b.load(std::memory_order_relaxed);
      uint64_t ts = TimestampOf(v);
      if (ts >= oldest && ts <= now_s) sum += CountOf(v);
    }
    return sum;
  }

 private:
  static uint64_t Pack(uint64_t ts, uint32_t count) {
    return (ts << 32) | count;
  }
  static uint64_t TimestampOf(uint64_t v) { return v >> 32; }
  static uint32_t CountOf(uint64_t v) { return static_cast<uint32_t>(v); }

  std::atomic<uint64_t> buckets_[kWindowSize];
};

class DirAccessStats;
using DirAccessStatsSPtr = std::shared_ptr<DirAccessStats>;

enum class DirAccessEvent : uint8_t {
  kLookupSubdir = 0,
  kLookupSubfile,
  kOpenDir,
  kOpenSubfileWrite,
  kOpenSubfileRead,
  kEventNum,
};

class AccessStatsWatcher {
 public:
  AccessStatsWatcher() = default;
  virtual ~AccessStatsWatcher() = default;

  virtual void OnWindowCountChanged(DirAccessEvent event, Ino ino,
                                    uint64_t count) {}
};

using AccessStatsWatcherUPtr = std::unique_ptr<AccessStatsWatcher>;

class DirAccessStats {
 public:
  DirAccessStats(Ino ino, const std::vector<AccessStatsWatcherUPtr>& watchers)
      : ino_(ino),
        watchers_(watchers),
        last_active_time_s_(utils::Timestamp()) {}
  ~DirAccessStats() = default;

  DirAccessStats(const DirAccessStats&) = delete;
  DirAccessStats& operator=(const DirAccessStats&) = delete;

  static DirAccessStatsSPtr New(
      Ino ino, const std::vector<AccessStatsWatcherUPtr>& watchers) {
    return std::make_shared<DirAccessStats>(ino, watchers);
  }

  void IncCount(DirAccessEvent event) {
    uint64_t count = counters_[static_cast<size_t>(event)].Inc();
    for (const auto& watcher : watchers_) {
      watcher->OnWindowCountChanged(event, ino_, count);
    }
  }

  void UpdateLastActiveTimeS() {
    last_active_time_s_.store(utils::Timestamp(), std::memory_order_relaxed);
  }
  uint64_t GetLastActiveTimeS() const {
    return last_active_time_s_.load(std::memory_order_relaxed);
  }

 private:
  const Ino ino_;

  SlidingWindow counters_[static_cast<size_t>(DirAccessEvent::kEventNum)];

  const std::vector<AccessStatsWatcherUPtr>& watchers_;

  std::atomic<uint64_t> last_active_time_s_{0};
};

class AccessStatsMap {
 public:
  AccessStatsMap() = default;
  ~AccessStatsMap() = default;

  AccessStatsMap(const AccessStatsMap&) = delete;
  AccessStatsMap& operator=(const AccessStatsMap&) = delete;

  DirAccessStatsSPtr GetOrCreate(Ino ino);

  void RegisterWatcher(AccessStatsWatcherUPtr watcher) {
    watchers_.emplace_back(std::move(watcher));
  }

  void CleanExpired(uint64_t expire_s);

  size_t Size();
  size_t Bytes();

  void Summary(Json::Value& value);

 private:
  using Map = absl::flat_hash_map<Ino, DirAccessStatsSPtr>;

  static constexpr size_t kShardNum = 64;
  mutable utils::Shards<Map, kShardNum> shard_map_;

  std::vector<AccessStatsWatcherUPtr> watchers_;

  // metric
  bvar::Adder<uint64_t> total_count_{"meta_access_stats_total_count"};
  bvar::Adder<uint64_t> clean_count_{"meta_access_stats_clean_count"};
};

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_MDS_STATISTICS_H_