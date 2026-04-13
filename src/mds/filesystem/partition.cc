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

#include <json/value.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "common/logging.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "mds/common/helper.h"
#include "utils/time.h"

namespace dingofs {
namespace mds {

static const std::string kPartitionMetricsPrefix = "dingofs_{}_partition_cache_{}";

// 0: no limit
DEFINE_uint32(mds_partition_cache_shard_max_count, 4 * 1024 * 1024, "partition cache shard max count");
DEFINE_uint32(mds_partition_dentry_op_max_count, 100000, "partition dentry op max count");

DEFINE_uint32(mds_partition_shard_split_threshold, 8192, "split shard when dentry count exceeds this");

// --- DirShard implementation ---

void DirShard::Put(const Dentry& dentry) {
  utils::WriteLockGuard lk(lock_);

  children_[dentry.Name()] = dentry;

  UpdateLastActiveTime();
}

void DirShard::Delete(const std::string& name) {
  utils::WriteLockGuard lk(lock_);

  children_.erase(name);

  UpdateLastActiveTime();
}

bool DirShard::Get(const std::string& name, Dentry& out) {
  utils::ReadLockGuard lk(lock_);

  auto it = children_.find(name);
  if (it == children_.end()) return false;

  out = it->second;

  UpdateLastActiveTime();

  return true;
}

void DirShard::Scan(const std::string& start_name, uint32_t limit, bool is_only_dir, std::vector<Dentry>& dentries) {
  utils::ReadLockGuard lk(lock_);

  limit = limit > 0 ? limit : UINT32_MAX;

  uint32_t count = 0;
  for (auto it = children_.lower_bound(start_name); it != children_.end() && count < limit; ++it) {
    if (is_only_dir && it->second.Type() != pb::mds::FileType::DIRECTORY) {
      continue;
    }

    dentries.push_back(it->second);
    ++count;
  }

  UpdateLastActiveTime();
}

bool DirShard::Contains(const std::string& name) const {
  if (range_.start.empty() && range_.end.empty()) return true;

  return name >= range_.start && (range_.end.empty() || name < range_.end);
}

bool DirShard::IsFull() const { return Size() >= FLAGS_mds_partition_shard_split_threshold; }

// get mid key of the shard, used for split
std::string DirShard::Mid() {
  utils::ReadLockGuard lk(lock_);

  if (children_.empty()) return "";

  auto it = children_.begin();
  std::advance(it, children_.size() / 2);

  return it->first;
}

bool DirShard::Empty() const {
  utils::ReadLockGuard lk(lock_);

  return children_.empty();
}

size_t DirShard::Size() const {
  utils::ReadLockGuard lk(lock_);

  return children_.size();
}

size_t DirShard::Bytes() const {
  utils::ReadLockGuard lk(lock_);

  return children_.size() * sizeof(Dentry);
}

// split shard into two by key, [start, key), [key, end)
std::pair<DirShardSPtr, DirShardSPtr> DirShard::Split(const std::string& key, uint64_t left_id, uint64_t right_id) {
  utils::WriteLockGuard lk(lock_);

  absl::btree_map<std::string, Dentry> left_dentries, right_dentries;
  for (auto& it : children_) {
    if (it.first < key) {
      left_dentries[it.first] = it.second;
    } else {
      right_dentries[it.first] = it.second;
    }
  }

  // [start, key), [key, end)
  Range left_range{range_.start, key};
  Range right_range{key, range_.end};

  DirShardSPtr left_shard = DirShard::New(left_id, left_range, version_, std::move(left_dentries));
  DirShardSPtr right_shard = DirShard::New(right_id, right_range, version_, std::move(right_dentries));

  return {left_shard, right_shard};
}

std::string DirShard::ToString() const {
  return fmt::format("id({}) range[{},{}) version({}) size({})", id_, Helper::StringToHex(range_.start),
                     Helper::StringToHex(range_.end), version_, Size());
}

void DirShard::Dump(Json::Value& value) const {
  utils::ReadLockGuard lk(lock_);

  value["id"] = id_;
  value["start"] = Helper::StringToHex(range_.start);
  value["end"] = Helper::StringToHex(range_.end);
  value["version"] = version_;
  value["size"] = Size();
}

// --- ShardPartition operations ---

uint64_t ShardPartition::BaseVersion() {
  utils::ReadLockGuard lk(lock_);

  return base_version_;
}

uint64_t ShardPartition::DeltaVersion() {
  utils::ReadLockGuard lk(lock_);

  return delta_version_;
}

InodeSPtr ShardPartition::ParentInode() {
  utils::ReadLockGuard lk(lock_);

  return inode_.lock();
}

void ShardPartition::SetParentInode(InodeSPtr parent_inode) {
  utils::WriteLockGuard lk(lock_);

  inode_ = parent_inode;
}

Status ShardPartition::Get(const std::string& name, Dentry& out) {
  do {
    Range range;
    auto shard = GetShard(name, range);
    if (shard != nullptr) {
      if (shard->IsFull()) AsyncSplitDirShard(range);

      if (shard->Get(name, out)) {
        return Status::OK();
      } else {
        return Status(pb::error::ENOT_FOUND, fmt::format("not found dentry({}/{})", ino_, name));
      }
    }

    // Shard not in cache, try to fetch
    auto status = FetchDirShard(range, shard);
    if (!status.ok()) return status;

  } while (true);

  // Unreachable
  return Status::OK();
}

std::vector<Dentry> ShardPartition::GetAll() {
  std::vector<Dentry> dentries;
  for (auto& it : shard_map_) {
    auto shard = it.second;
    shard->Scan("", UINT32_MAX, false, dentries);
  }

  return dentries;
}

Status ShardPartition::Scan(const std::string& trace_id, const std::string& start_name, uint32_t limit,
                            bool is_only_dir, std::vector<Dentry>& dentries) {
  limit = (limit > 0) ? limit : UINT32_MAX;

  std::string next_name = Helper::PrefixNext(start_name);
  do {
    Range range;
    auto shard = GetShard(next_name, range);
    if (shard == nullptr) {
      auto status = FetchDirShard(range, shard);
      if (!status.ok()) return status;
      continue;
    }

    uint32_t remaining = limit - static_cast<uint32_t>(dentries.size());
    size_t before_size = dentries.size();
    shard->Scan(next_name, remaining, is_only_dir, dentries);

    LOG(INFO) << fmt::format(
        "[partition.{}.{}.{}] scan dentry, shard({}) limit({}) before_size({}) after_size({}) start_name({}) range{}",
        fs_id_, ino_, trace_id, shard->ID(), limit, before_size, dentries.size(), start_name, range.ToString());

    if ((dentries.size() - before_size) < remaining && shard->IsLastShard()) break;

    next_name = shard->End();

    if (shard->IsFull()) AsyncSplitDirShard(range);

  } while (dentries.size() < limit);

  return Status::OK();
}

void ShardPartition::Put(const Dentry& dentry, uint64_t version) {
  {
    utils::WriteLockGuard lk(lock_);

    delta_version_ = std::max(version, delta_version_);
    AddDeltaOpNoLock({DentryOpType::ADD, version, dentry, 0});
  }

  auto shard = GetShard(dentry.Name());
  if (shard) shard->Put(dentry);
}

void ShardPartition::PutWithInode(const Dentry& dentry) {
  auto shard = GetShard(dentry.Name());
  if (shard) shard->Put(dentry);
}

void ShardPartition::Delete(const std::string& name, uint64_t version) {
  {
    utils::WriteLockGuard lk(lock_);

    delta_version_ = std::max(version, delta_version_);
    AddDeltaOpNoLock({DentryOpType::DELETE, version, Dentry(name), 0});
  }

  auto shard = GetShard(name);
  if (shard) shard->Delete(name);
}

void ShardPartition::Delete(const std::vector<std::string>& names, uint64_t version) {
  for (const auto& name : names) {
    auto shard = GetShard(name);
    if (shard) shard->Delete(name);
  }

  {
    utils::WriteLockGuard lk(lock_);

    delta_version_ = std::max(version, delta_version_);
    for (const auto& name : names) {
      AddDeltaOpNoLock({DentryOpType::DELETE, version, Dentry(name), 0});
    }
  }
}

bool ShardPartition::NeedCompact() {
  utils::ReadLockGuard lk(lock_);

  if (is_compacting_.load(std::memory_order_acquire)) return false;

  if (delta_dentry_ops_.size() <= FLAGS_mds_partition_dentry_op_max_count) return false;

  is_compacting_.store(true, std::memory_order_release);

  return true;
}

bool ShardPartition::Empty() const {
  utils::ReadLockGuard lk(lock_);

  for (const auto& it : shard_map_) {
    if (!it.second->Empty()) return false;
  }

  return true;
}

size_t ShardPartition::Size() const {
  utils::ReadLockGuard lk(lock_);

  size_t size = 0;
  for (const auto& it : shard_map_) {
    size += it.second->Size();
  }

  return size;
}

size_t ShardPartition::ShardSize() const {
  utils::ReadLockGuard lk(lock_);

  return shard_map_.size();
}

size_t ShardPartition::Bytes() const {
  utils::ReadLockGuard lk(lock_);

  size_t bytes = 0;
  for (const auto& it : shard_map_) {
    bytes += it.second->Bytes();
  }

  return bytes;
}

// --- ShardPartition private helpers ---

void ShardPartition::AddDeltaOpNoLock(DentryOp&& op) {
  op.time_s = utils::Timestamp();

  delta_dentry_ops_.push_back(std::move(op));

  // keep delta_dentry_ops_ ordered by version, the new op is usually with greater version, so we compare with the last
  // op first to avoid unnecessary sort
  if (delta_dentry_ops_.size() > 1) {
    auto it = delta_dentry_ops_.end();
    --it;  // last element
    auto prev = it;
    --prev;
    while (true) {
      if (it->version >= prev->version) break;
      std::iter_swap(it, prev);
      if (prev == delta_dentry_ops_.begin()) break;
      it = prev;
      --prev;
    }
  }
}

void ShardPartition::ApplyDeltaOpNoLock(DirShardSPtr shard) {
  for (auto& op : delta_dentry_ops_) {
    if (op.version <= shard->Version()) continue;

    // check shard range contains dentry name, if not, skip this op and let it be applied to next shard after split
    if (!shard->Contains(op.dentry.Name())) continue;

    if (op.op_type == DentryOpType::ADD) {
      shard->Put(op.dentry);

    } else if (op.op_type == DentryOpType::DELETE) {
      shard->Delete(op.dentry.Name());
    }
  }
}

Status ShardPartition::RunOperation(Operation* operation) {
  CHECK(operation != nullptr) << "operation is null.";

  if (!operation->IsBatchRun()) {
    return operation_processor_->RunAlone(operation);
  }

  bthread::CountdownEvent count_down(1);

  operation->SetEvent(&count_down);

  if (!operation_processor_->RunBatched(operation)) {
    return Status(pb::error::EINTERNAL, "commit mutation fail");
  }

  CHECK(count_down.wait() == 0) << "count down wait fail.";

  return operation->GetResult().status;
}

Range ShardPartition::QueryRangeNoLock(const std::string& name) {
  if (shard_boundaries_.empty()) return {};

  auto it = std::upper_bound(shard_boundaries_.begin(), shard_boundaries_.end(), name);  // NOLINT
  Range range;
  range.start = (it == shard_boundaries_.begin()) ? "" : *std::prev(it);
  range.end = (it == shard_boundaries_.end()) ? "" : *it;

  return range;
}

DirShardSPtr ShardPartition::GetShardNoLock(const Range& range) {
  auto it = shard_map_.find(range.start);
  return (it != shard_map_.end()) ? it->second : nullptr;
}

DirShardSPtr ShardPartition::GetShard(const std::string& name) {
  utils::ReadLockGuard lk(lock_);

  return GetShardNoLock(QueryRangeNoLock(name));
}

DirShardSPtr ShardPartition::GetShard(const std::string& name, Range& out_range) {
  utils::ReadLockGuard lk(lock_);

  out_range = QueryRangeNoLock(name);
  return GetShardNoLock(out_range);
}

void ShardPartition::PutShard(DirShardSPtr shard) {
  utils::WriteLockGuard lk(lock_);

  PutShardNoLock(shard);
}

void ShardPartition::PutShardNoLock(DirShardSPtr shard) {
  ApplyDeltaOpNoLock(shard);

  shard_map_[shard->Start()] = shard;
}

void ShardPartition::DeleteShard(const std::string& start) {
  utils::WriteLockGuard lk(lock_);

  shard_map_.erase(start);
}

void ShardPartition::DeleteShardNoLock(const std::string& start) { shard_map_.erase(start); }

Status ShardPartition::FetchDirShard(const Range& range, DirShardSPtr& out_shard) {
  utils::Duration duration;
  absl::btree_map<std::string, Dentry> dentries;
  Trace trace;
  ScanDirShardOperation operation(trace, fs_id_, ino_, range, [&dentries](const DentryEntry& dentry) -> bool {
    dentries[dentry.name()] = Dentry(dentry);
    return true;
  });

  auto status = RunOperation(&operation);
  if (!status.ok()) return status;

  auto& result = operation.GetResult();

  uint64_t version = result.attr.version();

  out_shard = DirShard::New(NextShardID(), range, version, std::move(dentries));

  PutShard(out_shard);

  LOG(INFO) << fmt::format("[partition.{}.{}][{}us] fetch dir shard, range{} {}.", fs_id_, ino_, duration.ElapsedUs(),
                           range.ToString(), out_shard->ToString());

  return Status::OK();
}

bool ShardPartition::Refresh(InodeSPtr inode) {
  CHECK(inode != nullptr) << fmt::format("inode is null, partition ino({})", ino_);
  CHECK(inode->Ino() == ino_) << fmt::format("inode ino({}) mismatch partition ino({})", inode->Ino(), ino_);

  LOG(INFO) << fmt::format(
      "[partition.{}.{}] refresh partition, version({}->{}) delta_version({}) shard_boundaries({}).", fs_id_, ino_,
      base_version_, inode->Version(), delta_version_, Helper::VectorToString(inode->ShardBoundaries()));

  utils::WriteLockGuard lk(lock_);

  if (inode->Version() <= base_version_) return false;

  base_version_ = inode->Version();
  delta_version_ = std::max(delta_version_, base_version_);
  shard_map_.clear();

  delta_version_ = base_version_;
  for (auto it = delta_dentry_ops_.begin(); it != delta_dentry_ops_.end();) {
    if (it->version <= base_version_) {
      it = delta_dentry_ops_.erase(it);
      continue;
    }

    delta_version_ = std::max(delta_version_, it->version);
    ++it;
  }

  is_compacting_.store(false, std::memory_order_release);

  return true;
}

Status ShardPartition::DoSplitDirShard(const Range& range) {
  auto shard = GetShard(range.start);
  if (shard == nullptr || !shard->IsFull()) return Status::OK();

  std::string mid_key = shard->Mid();
  if (mid_key.empty()) return Status::OK();

  // generate new shard boundaries
  std::vector<std::string> shard_boundaries;
  {
    utils::ReadLockGuard lk(lock_);
    shard_boundaries = shard_boundaries_;
  }
  // check if mid_key already exists in shard boundaries to avoid duplicate split
  if (std::binary_search(shard_boundaries.begin(), shard_boundaries.end(), mid_key)) {  // NOLINT
    return Status::OK();
  }
  auto it = std::upper_bound(shard_boundaries.begin(), shard_boundaries.end(), mid_key);  // NOLINT
  shard_boundaries.insert(it, mid_key);

  // update parent inode shard boundaries
  Trace trace;
  UpdateShardBoundariesOperation operation(trace, fs_id_, ino_, shard_boundaries);

  auto status = RunOperation(&operation);
  if (!status.ok()) return status;

  // split local shard and update cache
  DirShardSPtr left, right;
  {
    auto pair_shards = shard->Split(mid_key, NextShardID(), NextShardID());
    left = pair_shards.first;
    right = pair_shards.second;

    utils::WriteLockGuard lk(lock_);

    DeleteShardNoLock(shard->Start());
    PutShardNoLock(left);
    PutShardNoLock(right);

    shard_boundaries_ = std::move(shard_boundaries);
  }

  LOG(INFO) << fmt::format("[partition.{}.{}] split dir shard, parent({}) left({}) right({}) shard_boundaries({}).",
                           fs_id_, ino_, shard->ToString(), left->ToString(), right->ToString(),
                           Helper::VectorToString(shard_boundaries_));

  return Status::OK();
}

void ShardPartition::AsyncSplitDirShard(const Range& range) {
  // only allow one split at a time
  bool splitting = false;
  if (!is_splitting_.compare_exchange_strong(splitting, true)) return;

  struct Param {
    ShardPartition& partition;
    Range range;

    Param(ShardPartition& partition, const Range& range) : partition(partition), range(range) {}
  };

  Param* param = new Param(*this, range);

  bthread_t tid;
  const bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
  int ret = bthread_start_background(
      &tid, &attr,
      [](void* arg) -> void* {
        Param* param = static_cast<Param*>(arg);
        CHECK(param != nullptr) << "param is null.";
        auto& partition = param->partition;

        const uint32_t fs_id = partition.FsId();
        const Ino ino = partition.INo();

        auto status = partition.DoSplitDirShard(param->range);
        if (!status.ok()) {
          LOG(ERROR) << fmt::format("[partition.{}.{}] async split dir shard fail, error({}).", fs_id, ino,
                                    status.error_str());
        }

        partition.is_splitting_.store(false);

        delete param;

        return nullptr;
      },
      param);
  if (ret != 0) {
    is_splitting_.store(false);
    delete param;
    LOG(FATAL) << fmt::format("[partition.{}.{}] start bthread fail, error({}).", fs_id_, ino_, strerror(ret));
  }
}

size_t ShardPartition::CleanExpired(uint64_t expire_s) {
  utils::WriteLockGuard lk(lock_);

  size_t clean_count = 0;
  for (auto it = shard_map_.begin(); it != shard_map_.end();) {
    auto& shard = it->second;
    if (shard->LastActiveTimeS() < expire_s) {
      LOG(INFO) << fmt::format("[partition.{}.{}] clean expired shard, {}, last_active_time_s({}).", fs_id_, ino_,
                               shard->ToString(), shard->LastActiveTimeS());
      auto tmp_it = it++;
      shard_map_.erase(tmp_it);
      ++clean_count;
    } else {
      ++it;
    }
  }

  return clean_count;
}

void ShardPartition::Dump(Json::Value& value) const {
  utils::ReadLockGuard lk(lock_);

  value["fs_id"] = fs_id_;
  value["ino"] = ino_;
  value["base_version"] = base_version_;
  value["delta_version"] = delta_version_;
  value["delta_dentry_ops_count"] = delta_dentry_ops_.size();

  std::map<std::string, Json::Value> shard_map_value;
  if (shard_boundaries_.empty()) {
    Json::Value shard_value(Json::objectValue);
    shard_value["start"] = "";
    shard_value["end"] = "";
    shard_map_value[""] = shard_value;

  } else {
    for (size_t i = 0; i <= shard_boundaries_.size(); ++i) {
      Json::Value shard_value(Json::objectValue);
      if (i == 0) {
        shard_value["start"] = "";
        shard_value["end"] = shard_boundaries_[i];
        shard_map_value[""] = shard_value;
      } else if (i == shard_boundaries_.size()) {
        shard_value["start"] = shard_boundaries_[i - 1];
        shard_value["end"] = "";
        shard_map_value[shard_boundaries_[i - 1]] = shard_value;

      } else {
        shard_value["start"] = shard_boundaries_[i - 1];
        shard_value["end"] = shard_boundaries_[i];
        shard_map_value[shard_boundaries_[i - 1]] = shard_value;
      }
    }
  }

  for (const auto& [shard_key, shard] : shard_map_) {
    auto it = shard_map_value.find(shard_key);
    if (it == shard_map_value.end()) continue;

    auto& shard_value = it->second;
    shard_value["id"] = shard->ID();
    shard_value["size"] = shard->Size();
    shard_value["version"] = shard->Version();
  }

  Json::Value shards_value(Json::arrayValue);
  for (auto& it : shard_map_value) {
    shards_value.append(it.second);
  }

  value["shards"] = shards_value;
}

PartitionCache::PartitionCache(uint32_t fs_id)
    : fs_id_(fs_id),
      total_count_(fmt::format(kPartitionMetricsPrefix, fs_id, "total_count")),
      access_miss_count_(fmt::format(kPartitionMetricsPrefix, fs_id, "miss_count")),
      access_hit_count_(fmt::format(kPartitionMetricsPrefix, fs_id, "hit_count")),
      clean_count_(fmt::format(kPartitionMetricsPrefix, fs_id, "clean_count")) {}

PartitionCache::~PartitionCache() {}  // NOLINT

PartitionPtr PartitionCache::PutIf(const PartitionPtr& partition) {
  Ino ino = partition->INo();
  PartitionPtr new_partition = partition;

  shard_map_.withWLock(
      [&](Map& map) mutable {
        auto it = map.find(ino);
        if (it == map.end()) {
          map.emplace(ino, partition);
          total_count_ << 1;

        } else {
          it->second->Refresh(partition->ParentInode());
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

size_t PartitionCache::ShardSize() {
  size_t size = 0;
  shard_map_.iterate([&size](const Map& map) {
    for (const auto& [_, partition] : map) {
      size += partition->ShardSize();
    }
  });

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
  size_t shard_size = ShardSize();
  if (shard_size < FLAGS_mds_partition_cache_shard_max_count) return;

  size_t clean_count = 0;
  shard_map_.iterate([&](const Map& map) {
    for (const auto& [_, partition] : map) {
      clean_count += partition->CleanExpired(expire_s);
    }
  });

  clean_count_ << clean_count;

  LOG(INFO) << fmt::format("[cache.partition.{}] clean expired, stat({}|{}|{}).", fs_id_, shard_size, clean_count,
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