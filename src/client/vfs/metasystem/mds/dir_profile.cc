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

#include "client/vfs/metasystem/mds/dir_profile.h"

#include <cstdint>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "common/options/client.h"
#include "utils/concurrent/concurrent.h"
#include "utils/time.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

static const uint32_t kWarmupDirMinFiles = 100;
static const uint32_t kWarmupOpenThreshold = 16;
static const uint32_t kWarmupOpenWindowS = 16;

void DirProfile::Accumulate(Ino child_ino, FileType type, uint64_t length) {
  utils::WriteLockGuard lk(lock_);

  ++total_children_;

  if (type != FileType::kFile) return;
  if (length > FLAGS_vfs_tiny_file_max_size) return;

  small_file_inos_.push_back(child_ino);
}

void DirProfile::Finalize() {
  utils::WriteLockGuard lk(lock_);

  uint32_t small_file_count = small_file_inos_.size();
  is_small_file_dir_ =
      (total_children_ > 0 && small_file_count >= kWarmupDirMinFiles);

  expire_ts_ = utils::Timestamp() + FLAGS_vfs_meta_warmup_small_file_ttl_s;
  is_finalized_ = true;

  LOG_DEBUG << fmt::format(
      "[meta.dir_profile.{}.{}] finalize dir profile, total_children({}),"
      "small_file_count({}),is_small_file_dir({}).",
      parent_ino_, fh_, total_children_, small_file_inos_.size(),
      is_small_file_dir_);
}

std::vector<Ino> DirProfile::CheckAndGenWarmupInos(Ino ino) {
  utils::WriteLockGuard lk(lock_);

  uint64_t now_s = utils::Timestamp();
  last_active_time_s_ = now_s;

  if (now_s > (open_window_start_s_ + kWarmupOpenWindowS)) {
    open_window_start_s_ = now_s;
    open_inos_.clear();
  }

  open_inos_.insert(ino);

  if (open_inos_.size() < kWarmupOpenThreshold) {
    return {};
  }

  return GenWarmupInos();
}

std::vector<Ino> DirProfile::GenWarmupInos() {
  std::vector<Ino> candidates;
  candidates.reserve(FLAGS_vfs_meta_warmup_small_file_batch_size);

  uint32_t i = warmed_pos_;
  for (; i < small_file_inos_.size(); ++i) {
    Ino ino = small_file_inos_[i];
    if (candidates.size() >= FLAGS_vfs_meta_warmup_small_file_batch_size) break;
    if (warmed_inos_.contains(ino)) continue;
    if (open_inos_.contains(ino)) continue;

    candidates.push_back(ino);
  }
  warmed_pos_ = i;

  open_window_start_s_ = utils::Timestamp();
  open_inos_.clear();

  if (!candidates.empty()) {
    LOG_DEBUG << fmt::format(
        "[meta.dir_profile.{}.{}] gen warmup inodes, candidates({}), "
        "total_small_files({}),warmed_count({}).",
        parent_ino_, fh_, candidates.size(), small_file_inos_.size(),
        warmed_inos_.size());
  }

  return candidates;
}

std::vector<Ino> DirProfile::SmallFileInosForTest() const {
  utils::ReadLockGuard lk(lock_);
  return std::vector<Ino>(small_file_inos_.begin(), small_file_inos_.end());
}

void DirProfileCache::Put(DirProfileSPtr& dir_profile) {
  // if (!dir_profile->IsSmallFileDir()) return;

  Ino ino = dir_profile->ParentIno();

  bool is_put = false;
  shard_map_.withWLock(
      [ino, &dir_profile, &is_put](Map& map) {
        auto it = map.find(ino);
        if (it == map.end()) {
          map.emplace(ino, dir_profile);
          is_put = true;

        } else {
          auto pre_profile = it->second;
          if (dir_profile->TotalChildren() > pre_profile->TotalChildren()) {
            it->second = dir_profile;
            is_put = true;
          }
        }
      },
      ino);

  if (is_put) {
    LOG_DEBUG << fmt::format(
        "[meta.dir_profile.{}.{}] put dir profile, total_children({}).", ino,
        dir_profile->Fh(), dir_profile->TotalChildren());
  }
}

DirProfileSPtr DirProfileCache::Get(Ino ino) {
  DirProfileSPtr profile;
  shard_map_.withRLock(
      [ino, &profile](Map& map) {
        auto it = map.find(ino);
        if (it != map.end()) profile = it->second;
      },
      ino);
  return profile;
}

void DirProfileCache::Erase(Ino ino) {
  shard_map_.withWLock([ino](Map& map) { map.erase(ino); }, ino);
}

void DirProfileCache::CleanExpired(uint64_t expire_s) {
  std::vector<Ino> dir_profiles;
  shard_map_.iterate([&](const Map& map) {
    for (const auto& [ino, profile] : map) {
      if (profile->LastActiveTimeS() <= expire_s) dir_profiles.push_back(ino);
    }
  });

  for (Ino ino : dir_profiles) Erase(ino);
}

size_t DirProfileCache::Size() const {
  size_t size = 0;
  shard_map_.iterate([&size](const Map& map) { size += map.size(); });
  return size;
}

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs
