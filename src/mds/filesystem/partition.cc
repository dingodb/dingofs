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

#include "mds/filesystem/partition.h"

#include <algorithm>
#include <cstdint>
#include <memory>

#include "fmt/format.h"
#include "mds/common/logging.h"

namespace dingofs {
namespace mds {

static const std::string kPartitionMetricsPrefix = "dingofs_{}_partition_cache_";

// 0: no limit
DEFINE_uint32(mds_partition_cache_max_count, 4 * 1024 * 1024, "partition cache max count");

const uint32_t kDentryDefaultNum = 1024;

uint64_t Partition::Version() {
  utils::ReadLockGuard lk(lock_);

  return version_;
}

InodeSPtr Partition::ParentInode() {
  utils::ReadLockGuard lk(lock_);

  return inode_.lock();
}

void Partition::SetParentInode(InodeSPtr parent_inode) {
  utils::WriteLockGuard lk(lock_);

  inode_ = parent_inode;
}

void Partition::PutChild(const Dentry& dentry, uint64_t version) {
  utils::WriteLockGuard lk(lock_);

  auto it = children_.find(dentry.Name());
  if (it != children_.end()) {
    it->second = dentry;
  } else {
    children_[dentry.Name()] = dentry;
  }

  version_ = std::max(version, version_);
}

void Partition::DeleteChild(const std::string& name, uint64_t version) {
  utils::WriteLockGuard lk(lock_);

  children_.erase(name);

  version_ = std::max(version, version_);
}

void Partition::DeleteChildIf(const std::string& name, Ino ino, uint64_t version) {
  utils::WriteLockGuard lk(lock_);

  auto it = children_.find(name);
  if (it != children_.end() && it->second.INo() == ino) {
    children_.erase(it);
  }

  version_ = std::max(version, version_);
}

bool Partition::HasChild() {
  utils::ReadLockGuard lk(lock_);

  return !children_.empty();
}

bool Partition::GetChild(const std::string& name, Dentry& dentry) {
  utils::ReadLockGuard lk(lock_);

  auto it = children_.find(name);
  if (it == children_.end()) {
    return false;
  }

  dentry = it->second;
  return true;
}

std::vector<Dentry> Partition::GetChildren(const std::string& start_name, uint32_t limit, bool is_only_dir) {
  utils::ReadLockGuard lk(lock_);

  limit = limit > 0 ? limit : UINT32_MAX;

  std::vector<Dentry> dentries;
  dentries.reserve(kDentryDefaultNum);

  for (auto it = children_.upper_bound(start_name); it != children_.end() && dentries.size() < limit; ++it) {
    if (is_only_dir && it->second.Type() != pb::mds::FileType::DIRECTORY) {
      continue;
    }

    dentries.push_back(it->second);
  }

  return dentries;
}

std::vector<Dentry> Partition::GetAllChildren() {
  utils::ReadLockGuard lk(lock_);

  std::vector<Dentry> dentries;
  dentries.reserve(children_.size());

  for (const auto& [name, dentry] : children_) {
    dentries.push_back(dentry);
  }

  return dentries;
}

PartitionCache::PartitionCache(uint32_t fs_id)
    : fs_id_(fs_id),
      cache_(FLAGS_mds_partition_cache_max_count,
             std::make_shared<utils::CacheMetrics>(fmt::format(kPartitionMetricsPrefix, fs_id))) {}
PartitionCache::~PartitionCache() {}  // NOLINT

void PartitionCache::PutIf(Ino ino, PartitionPtr partition) {
  cache_.PutIf(ino, partition,
               [&](PartitionPtr& old_partition) { return old_partition->Version() < partition->Version(); });
}

void PartitionCache::Delete(Ino ino) {
  DINGO_LOG(INFO) << fmt::format("[cache.partition.{}] delete partition ino({}).", fs_id_, ino);

  cache_.Remove(ino);
}

void PartitionCache::DeleteIf(Ino ino, uint64_t version) {
  cache_.RemoveIf(ino, [&](const PartitionPtr& partition) { return partition->Version() < version; });
}

void PartitionCache::BatchDeleteInodeIf(const std::function<bool(const Ino&)>& f) {
  DINGO_LOG(INFO) << fmt::format("[cache.partition.{}] batch delete inode.", fs_id_);

  cache_.BatchRemoveIf(f);
}

void PartitionCache::Clear() {
  DINGO_LOG(INFO) << fmt::format("[cache.partition.{}] clear.", fs_id_);

  cache_.Clear();
}

PartitionPtr PartitionCache::Get(Ino ino) {
  PartitionPtr partition;
  if (!cache_.Get(ino, &partition)) {
    return nullptr;
  }

  return partition;
}

std::map<uint64_t, PartitionPtr> PartitionCache::GetAll() { return cache_.GetAll(); }

void PartitionCache::DescribeByJson(Json::Value& value) {
  const auto metrics = cache_.GetCacheMetrics();

  value["cache_count"] = metrics->cacheCount.get_value();
  value["cache_bytes"] = metrics->cacheBytes.get_value();
  value["cache_hit"] = metrics->cacheHit.get_value();
  value["cache_miss"] = metrics->cacheMiss.get_value();
}

}  // namespace mds
}  // namespace dingofs