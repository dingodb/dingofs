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

#include "mdsv2/filesystem/partition.h"

#include <memory>

namespace dingofs {
namespace mdsv2 {

static const std::string kPartitionMetricsPrefix = "dingofs_partition_cache_";

// 0: no limit
DEFINE_uint32(partition_cache_max_count, 0, "partition cache max count");

InodeSPtr Partition::ParentInode() {
  CHECK(parent_inode_ != nullptr) << "parent inode is null.";

  return parent_inode_;
}

void Partition::PutChild(const Dentry& dentry) {
  utils::WriteLockGuard lk(lock_);

  auto it = children_.find(dentry.Name());
  if (it != children_.end()) {
    it->second = dentry;
  } else {
    children_[dentry.Name()] = dentry;
  }
}

void Partition::DeleteChild(const std::string& name) {
  utils::WriteLockGuard lk(lock_);

  children_.erase(name);
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

  std::vector<Dentry> dentries;
  dentries.reserve(limit);

  for (auto it = children_.upper_bound(start_name); it != children_.end() && dentries.size() < limit; ++it) {
    if (is_only_dir && it->second.Type() != pb::mdsv2::FileType::DIRECTORY) {
      continue;
    }

    dentries.push_back(it->second);
  }

  return std::move(dentries);
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

PartitionCache::PartitionCache()
    : cache_(FLAGS_partition_cache_max_count, std::make_shared<utils::CacheMetrics>(kPartitionMetricsPrefix)) {}
PartitionCache::~PartitionCache() {}  // NOLINT

void PartitionCache::Put(uint64_t ino, PartitionPtr partition) { cache_.Put(ino, partition); }

void PartitionCache::Delete(uint64_t ino) { cache_.Remove(ino); }

PartitionPtr PartitionCache::Get(uint64_t ino) {
  PartitionPtr partition;
  if (!cache_.Get(ino, &partition)) {
    return nullptr;
  }

  return partition;
}

std::map<uint64_t, PartitionPtr> PartitionCache::GetAll() { return cache_.GetAll(); }

}  // namespace mdsv2
}  // namespace dingofs