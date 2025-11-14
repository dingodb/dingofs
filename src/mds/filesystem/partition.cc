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

#include <glog/logging.h>

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

uint64_t Partition::BaseVersion() {
  utils::ReadLockGuard lk(lock_);

  return base_version_;
}

uint64_t Partition::DeltaVersion() {
  utils::ReadLockGuard lk(lock_);

  return delta_version_;
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

  delta_dentry_ops_.push_back(DentryOp{DentryOpType::ADD, version, dentry});

  delta_version_ = std::max(version, delta_version_);
}

void Partition::DeleteChild(const std::string& name, uint64_t version) {
  utils::WriteLockGuard lk(lock_);

  Dentry detry(name);
  auto it = children_.find(name);
  if (it != children_.end()) {
    detry = it->second;
    children_.erase(it);
  }

  delta_dentry_ops_.push_back(DentryOp{DentryOpType::DELETE, version, detry});

  delta_version_ = std::max(version, delta_version_);
}

void Partition::DeleteChildIf(const std::string& name, Ino ino, uint64_t version) {
  utils::WriteLockGuard lk(lock_);

  auto it = children_.find(name);
  if (it != children_.end() && it->second.INo() == ino) {
    delta_dentry_ops_.push_back(DentryOp{DentryOpType::DELETE, version, it->second});
    children_.erase(it);
  }

  delta_version_ = std::max(version, delta_version_);
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

bool Partition::Merge(PartitionPtr& other_partition) {
  CHECK(other_partition->ino_ == ino_) << "merge partition error, ino not match.";

  utils::WriteLockGuard lk(lock_);

  if (other_partition->BaseVersion() <= base_version_) return false;

  DINGO_LOG(INFO) << fmt::format("[partition.{}] merge, self({},{},{},{}) other({},{}).", ino_, base_version_,
                                 delta_version_, children_.size(), delta_dentry_ops_.size(),
                                 other_partition->BaseVersion(), other_partition->children_.size());

  base_version_ = other_partition->BaseVersion();
  children_.swap(other_partition->children_);

  // apply delta ops
  delta_dentry_ops_.sort([](const DentryOp& a, const DentryOp& b) -> bool { return a.version < b.version; });

  delta_version_ = base_version_;
  for (const auto& op : delta_dentry_ops_) {
    if (op.version <= base_version_) continue;

    if (op.op_type == DentryOpType::ADD) {
      children_[op.dentry.Name()] = op.dentry;

    } else if (op.op_type == DentryOpType::DELETE) {
      children_.erase(op.dentry.Name());
    }

    delta_version_ = std::max(delta_version_, op.version);
  }

  delta_dentry_ops_.clear();

  return true;
}

PartitionCache::PartitionCache(uint32_t fs_id)
    : fs_id_(fs_id),
      cache_(FLAGS_mds_partition_cache_max_count,
             std::make_shared<utils::CacheMetrics>(fmt::format(kPartitionMetricsPrefix, fs_id))) {}
PartitionCache::~PartitionCache() {}  // NOLINT

PartitionPtr PartitionCache::PutIf(Ino ino, PartitionPtr partition) {
  PartitionPtr new_partition = partition;
  cache_.PutInplaceIf(ino, partition, [&](PartitionPtr& old_partition) {
    old_partition->Merge(partition);
    new_partition = old_partition;
  });

  return new_partition;
}

void PartitionCache::Delete(Ino ino) {
  DINGO_LOG(INFO) << fmt::format("[cache.partition.{}] delete partition ino({}).", fs_id_, ino);

  cache_.Remove(ino);
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