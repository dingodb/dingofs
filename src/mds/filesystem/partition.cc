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
#include <atomic>
#include <cstdint>
#include <vector>

#include "common/logging.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "utils/time.h"

namespace dingofs {
namespace mds {

static const std::string kPartitionMetricsPrefix = "dingofs_{}_partition_cache_{}";

// 0: no limit
DEFINE_uint32(mds_partition_cache_max_count, 4 * 1024 * 1024, "partition cache max count");
DEFINE_uint32(mds_partition_dentry_op_expire_interval_s, 600, "partition dentry op expire interval seconds");

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

void Partition::Put(const Dentry& dentry, uint64_t version) {
  LOG_DEBUG << fmt::format("[partition.{}] put child, name({}), version({}) this({}).", ino_, dentry.Name(), version,
                           (void*)this);

  utils::WriteLockGuard lk(lock_);

  auto it = children_.find(dentry.Name());
  if (it != children_.end()) {
    it->second = dentry;
  } else {
    children_[dentry.Name()] = dentry;

    AddDeltaDentryOp(DentryOp{DentryOpType::ADD, version, dentry});
  }

  delta_version_ = std::max(version, delta_version_);
}

void Partition::PutWithInode(const Dentry& dentry) {
  utils::WriteLockGuard lk(lock_);

  auto it = children_.find(dentry.Name());
  if (it != children_.end()) {
    it->second = dentry;
  }
}

void Partition::Delete(const std::string& name, uint64_t version) {
  utils::WriteLockGuard lk(lock_);

  Dentry detry(name);
  auto it = children_.find(name);
  if (it != children_.end()) {
    detry = it->second;
    children_.erase(it);
  }

  AddDeltaDentryOp(DentryOp{DentryOpType::DELETE, version, detry});

  delta_version_ = std::max(version, delta_version_);
}

void Partition::Delete(const std::vector<std::string>& names, uint64_t version) {
  utils::WriteLockGuard lk(lock_);

  for (const auto& name : names) {
    Dentry detry(name);
    auto it = children_.find(name);
    if (it != children_.end()) {
      detry = it->second;
      children_.erase(it);
    }

    AddDeltaDentryOp(DentryOp{DentryOpType::DELETE, version, detry});
  }

  delta_version_ = std::max(version, delta_version_);
}

bool Partition::Empty() {
  utils::ReadLockGuard lk(lock_);

  return !children_.empty();
}

size_t Partition::Size() {
  utils::ReadLockGuard lk(lock_);

  return children_.size();
}

size_t Partition::Bytes() {
  utils::ReadLockGuard lk(lock_);

  return sizeof(Partition) + (children_.size() * sizeof(Dentry)) + (delta_dentry_ops_.size() * sizeof(DentryOp));
}

bool Partition::Get(const std::string& name, Dentry& dentry) {
  utils::ReadLockGuard lk(lock_);

  auto it = children_.find(name);
  if (it == children_.end()) return false;

  dentry = it->second;
  return true;
}

std::vector<Dentry> Partition::Scan(const std::string& start_name, uint32_t limit, bool is_only_dir) {
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

std::vector<Dentry> Partition::GetAll() {
  utils::ReadLockGuard lk(lock_);

  std::vector<Dentry> dentries;
  dentries.reserve(children_.size());

  for (const auto& [name, dentry] : children_) {
    dentries.push_back(dentry);
  }

  return dentries;
}

bool Partition::Merge(PartitionPtr& other_partition) { return Merge(std::move(*other_partition)); }

bool Partition::Merge(Partition&& other_partition) {  // NOLINT
  CHECK(other_partition.ino_ == ino_) << "merge partition error, ino not match.";

  utils::WriteLockGuard lk(lock_);

  if (other_partition.BaseVersion() <= base_version_) return false;

  LOG_DEBUG << fmt::format("[partition.{}] merge, self({},{},{},{}) other({},{}).", ino_, base_version_, delta_version_,
                           children_.size(), delta_dentry_ops_.size(), other_partition.BaseVersion(),
                           other_partition.children_.size());
  base_version_ = other_partition.BaseVersion();
  children_.swap(other_partition.children_);

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

void Partition::UpdateLastActiveTime() { last_active_time_s_.store(utils::Timestamp(), std::memory_order_relaxed); }

uint64_t Partition::LastActiveTimeS() { return last_active_time_s_.load(std::memory_order_relaxed); }

void Partition::AddDeltaDentryOp(DentryOp&& op) {
  op.time_s = utils::Timestamp();
  delta_dentry_ops_.push_back(std::move(op));

  // clean expired ops
  uint64_t now_s = utils::Timestamp();
  for (auto it = delta_dentry_ops_.begin(); it != delta_dentry_ops_.end();) {
    if (it->time_s + FLAGS_mds_partition_dentry_op_expire_interval_s < now_s) {
      it = delta_dentry_ops_.erase(it);
    } else {
      ++it;
    }
  }
}

PartitionCache::PartitionCache(uint32_t fs_id)
    : fs_id_(fs_id),
      total_count_(fmt::format(kPartitionMetricsPrefix, fs_id, "total_count")),
      access_miss_count_(fmt::format(kPartitionMetricsPrefix, fs_id, "miss_count")),
      access_hit_count_(fmt::format(kPartitionMetricsPrefix, fs_id, "hit_count")),
      clean_count_(fmt::format(kPartitionMetricsPrefix, fs_id, "clean_count")) {}

PartitionCache::~PartitionCache() {}  // NOLINT

PartitionPtr PartitionCache::PutIf(PartitionPtr& partition) {
  Ino ino = partition->INo();
  PartitionPtr new_partition = partition;

  shard_map_.withWLock(
      [&](Map& map) mutable {
        auto it = map.find(ino);
        if (it == map.end()) {
          map.emplace(ino, partition);
          total_count_ << 1;

        } else {
          it->second->Merge(partition);
          new_partition = it->second;
        }
      },
      ino);

  LOG_DEBUG << fmt::format("[cache.partition.{}.{}] putif, this({}).", fs_id_, ino, (void*)new_partition.get());

  return new_partition;
}

PartitionPtr PartitionCache::PutIf(Partition&& partition) {  // NOLINT
  Ino ino = partition.INo();
  PartitionPtr new_partition;

  shard_map_.withWLock(
      [&](Map& map) mutable {
        auto it = map.find(ino);
        if (it == map.end()) {
          new_partition = Partition::New(std::move(partition));
          map.insert(std::make_pair(ino, new_partition));

          total_count_ << 1;

        } else {
          it->second->Merge(std::move(partition));
          new_partition = it->second;
        }
      },
      ino);

  LOG_DEBUG << fmt::format("[cache.partition.{}.{}] putif, this({}).", fs_id_, ino, (void*)new_partition.get());

  return new_partition;
}

void PartitionCache::Delete(Ino ino) {
  LOG(INFO) << fmt::format("[cache.partition.{}] delete partition ino({}).", fs_id_, ino);

  shard_map_.withWLock([ino](Map& map) { map.erase(ino); }, ino);
}

void PartitionCache::DeleteIf(std::function<bool(const Ino&)>&& f) {  // NOLINT
  LOG(INFO) << fmt::format("[cache.partition.{}] batch delete inode.", fs_id_);

  shard_map_.iterateWLock([&](Map& map) {
    for (auto it = map.begin(); it != map.end();) {
      if (f(it->first)) {
        auto temp_it = it++;
        map.erase(temp_it);
      } else {
        ++it;
      }
    }
  });
}

void PartitionCache::Clear() {
  LOG(INFO) << fmt::format("[cache.partition.{}] clear.", fs_id_);

  shard_map_.iterateWLock([&](Map& map) { map.clear(); });
}

PartitionPtr PartitionCache::Get(Ino ino) {
  PartitionPtr partition;

  shard_map_.withRLock(
      [ino, &partition](Map& map) {
        auto it = map.find(ino);
        if (it != map.end()) {
          partition = it->second;
        }
      },
      ino);

  if (partition != nullptr) {
    partition->UpdateLastActiveTime();
    access_hit_count_ << 1;

  } else {
    access_miss_count_ << 1;
  }

  return partition;
}

std::vector<PartitionPtr> PartitionCache::GetAll() {
  std::vector<PartitionPtr> partitions;

  shard_map_.iterate([&partitions](const Map& map) {
    for (const auto& [_, partition] : map) {
      partitions.push_back(partition);
    }
  });

  return partitions;
}

size_t PartitionCache::Size() {
  size_t size = 0;
  shard_map_.iterate([&size](const Map& map) { size += map.size(); });

  return size;
}

size_t PartitionCache::Bytes() {
  size_t bytes = 0;
  shard_map_.iterate([&bytes](const Map& map) {
    for (const auto& [_, partition] : map) {
      bytes += partition->Bytes();
    }
  });

  return bytes;
}

void PartitionCache::CleanExpired(uint64_t expire_s) {
  if (Size() < FLAGS_mds_partition_cache_max_count) return;

  std::vector<PartitionPtr> partitions;
  shard_map_.iterate([&](const Map& map) {
    for (const auto& [_, partition] : map) {
      if (partition->LastActiveTimeS() < expire_s) {
        partitions.push_back(partition);
      }
    }
  });

  for (const auto& partition : partitions) {
    Delete(partition->INo());
  }

  clean_count_ << partitions.size();

  LOG(INFO) << fmt::format("[cache.partition.{}] clean expired, stat({}|{}|{}).", fs_id_, Size(), partitions.size(),
                           clean_count_.get_value());
}

void PartitionCache::DescribeByJson(Json::Value& value) {
  value["cache_count"] = Size();

  value["cache_hit"] = access_hit_count_.get_value();
  value["cache_miss"] = access_miss_count_.get_value();
  value["cache_clean"] = clean_count_.get_value();
}

void PartitionCache::Summary(Json::Value& value) {
  value["name"] = "partitioncache";
  value["count"] = Size();
  value["bytes"] = Bytes();
  value["total_count"] = total_count_.get_value();
  value["clean_count"] = clean_count_.get_value();
  value["hit_count"] = access_hit_count_.get_value();
  value["miss_count"] = access_miss_count_.get_value();
}

}  // namespace mds
}  // namespace dingofs