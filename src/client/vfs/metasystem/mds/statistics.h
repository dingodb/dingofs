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
#include "client/vfs/vfs_meta.h"
#include "json/value.h"
#include "utils/shards.h"
#include "utils/time.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

static constexpr uint32_t kWindowSize = 4;

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
    auto& counter = counters_[static_cast<size_t>(event)];
    counter.total_count << 1;
    counter.window_count << 1;

    uint64_t count = counter.window_count.get_value();
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
  struct Counter {
    bvar::Adder<uint64_t> total_count;
    bvar::WindowEx<bvar::Adder<uint32_t>, kWindowSize> window_count;
  };

  const Ino ino_;

  Counter counters_[static_cast<size_t>(DirAccessEvent::kEventNum)];

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