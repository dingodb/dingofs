// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_MDS_DIR_PROFILE_H_
#define DINGOFS_SRC_CLIENT_VFS_META_MDS_DIR_PROFILE_H_

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <vector>

#include "absl/container/btree_map.h"
#include "absl/container/flat_hash_set.h"
#include "client/vfs/vfs_meta.h"
#include "mds/common/type.h"
#include "utils/concurrent/concurrent.h"
#include "utils/shards.h"
#include "utils/time.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

using mds::Ino;

// Per-directory profile populated incrementally during ReadDir and consulted
// on Open to decide whether to fire a batch warmup. Best-effort: data is
// statistical, never authoritative for correctness.
class DirProfile {
 public:
  explicit DirProfile(Ino parent_ino) : parent_ino_(parent_ino) {}

  Ino ParentIno() const { return parent_ino_; }

  // Add one ReadDir entry into the running statistics. Idempotent on the
  // child ino (a duplicate page won't double-count or grow small_file_inos_).
  void Accumulate(Ino child_ino, FileType type, uint64_t length);

  void Finalize();

  std::vector<Ino> CheckAndGenWarmupInos(Ino ino);

  bool IsFinalized() const {
    utils::ReadLockGuard lk(lock_);
    return is_finalized_;
  }
  bool IsSmallFileDir() const {
    utils::ReadLockGuard lk(lock_);
    return is_small_file_dir_;
  }

  uint64_t TotalChildren() const {
    utils::ReadLockGuard lk(lock_);
    return total_children_;
  }
  uint64_t SmallFileCount() const {
    utils::ReadLockGuard lk(lock_);
    return small_file_inos_.size();
  }
  uint64_t WarmedCount() const {
    utils::ReadLockGuard lk(lock_);
    return warmed_inos_.size();
  }

  uint64_t LastActiveTimeS() const {
    utils::ReadLockGuard lk(lock_);
    return last_active_time_s_;
  }

  // Pure accessor for tests and metrics.
  std::vector<Ino> SmallFileInosForTest() const;

 private:
  std::vector<Ino> GenWarmupInos();

  mutable utils::RWLock lock_;

  const Ino parent_ino_;

  bool is_finalized_{false};
  bool is_small_file_dir_{false};
  uint64_t expire_ts_{0};

  uint64_t total_children_{0};

  std::vector<Ino> small_file_inos_;
  uint32_t warmed_pos_{0};

  absl::flat_hash_set<Ino> warmed_inos_;

  uint64_t open_window_start_s_{0};
  absl::flat_hash_set<Ino> open_inos_;

  uint64_t last_active_time_s_{0};
};

using DirProfileSPtr = std::shared_ptr<DirProfile>;

class DirProfileCache;
using DirProfileCacheUPtr = std::unique_ptr<DirProfileCache>;

// Sharded LRU+TTL container for DirProfile, modeled after InodeCache.
class DirProfileCache {
 public:
  DirProfileCache() = default;
  ~DirProfileCache() = default;

  DirProfileCache(const DirProfileCache&) = delete;
  DirProfileCache& operator=(const DirProfileCache&) = delete;

  static DirProfileCacheUPtr New() {
    return std::make_unique<DirProfileCache>();
  }

  void Put(DirProfileSPtr& dir_profile);

  // Returns existing entry or nullptr; never creates.
  DirProfileSPtr Get(Ino parent);

  void Erase(Ino parent);

  void CleanExpired(uint64_t expire_s);

  size_t Size() const;

 private:
  using Map = absl::btree_map<Ino, DirProfileSPtr>;

  static constexpr size_t kShardNum = 32;
  mutable utils::Shards<Map, kShardNum> shard_map_;
};

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_MDS_DIR_PROFILE_H_
