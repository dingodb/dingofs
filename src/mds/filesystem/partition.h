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

#ifndef DINGOFS_MDS_FILESYSTEM_PARTITION_H_
#define DINGOFS_MDS_FILESYSTEM_PARTITION_H_

#include <glog/logging.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/btree_map.h"
#include "json/value.h"
#include "mds/common/status.h"
#include "mds/common/type.h"
#include "mds/filesystem/dentry.h"
#include "mds/filesystem/inode.h"
#include "mds/filesystem/store_operation.h"

namespace dingofs {
namespace mds {

class DirShard;
using DirShardSPtr = std::shared_ptr<DirShard>;

class DirShard {
 public:
  DirShard(uint64_t id, const Range& range, uint64_t version, const std::vector<Dentry>& dentries)
      : id_(id), range_{range}, version_(version) {
    // ingest dentries to map
    for (const auto& dentry : dentries) {
      CHECK(Contains(dentry.Name())) << fmt::format("dentry name({}) out of shard range{}.", dentry.Name(),
                                                    range_.ToString());
      children_[dentry.Name()] = dentry;
    }
    last_active_time_s_ = utils::Timestamp();
  }
  DirShard(uint64_t id, const Range& range, uint64_t version, absl::btree_map<std::string, Dentry>&& dentries)
      : id_(id), range_{range}, version_(version) {
    // ingest dentries to map
    children_ = std::move(dentries);
    last_active_time_s_ = utils::Timestamp();
  }

  static DirShardSPtr New(uint64_t id, const Range& range, uint64_t version, const std::vector<Dentry>& dentries) {
    return std::make_shared<DirShard>(id, range, version, dentries);
  }

  static DirShardSPtr New(uint64_t id, const Range& range, uint64_t version,
                          absl::btree_map<std::string, Dentry>&& dentries) {
    return std::make_shared<DirShard>(id, range, version, std::move(dentries));
  }

  uint64_t ID() const { return id_; }

  // dentry operations
  void Put(const Dentry& dentry);
  void Delete(const std::string& name);

  bool Get(const std::string& name, Dentry& out);
  void Scan(const std::string& start_name, uint32_t limit, bool is_only_dir, std::vector<Dentry>& dentries);

  // range
  const std::string& Start() const { return range_.start; }
  const std::string& End() const { return range_.end; }
  bool Contains(const std::string& name) const;
  bool IsLastShard() const { return range_.end.empty(); }

  // if dentry count exceeds threshold, it's considered as full and need split
  bool IsFull() const;

  // get mid key of the shard, used for split
  std::string Mid();

  bool Empty() const;
  size_t Size() const;
  size_t Bytes() const;

  void UpdateLastActiveTime() { last_active_time_s_.store(utils::Timestamp(), std::memory_order_relaxed); }
  uint64_t LastActiveTimeS() { return last_active_time_s_.load(std::memory_order_relaxed); }

  uint64_t Version() const { return version_; }

  std::pair<DirShardSPtr, DirShardSPtr> Split(const std::string& key, uint64_t left_id, uint64_t right_id);

  std::string ToString() const;

  void Dump(Json::Value& value) const;

 private:
  const uint64_t id_;
  const Range range_;  // [start, end)
  const uint64_t version_;

  mutable utils::RWLock lock_;
  absl::btree_map<std::string, Dentry> children_;

  std::atomic<uint64_t> last_active_time_s_{0};
};

class ShardPartition;
using PartitionPtr = std::shared_ptr<ShardPartition>;

class ShardPartition {
 public:
  ShardPartition(OperationProcessorSPtr operation_processor, InodeSPtr& inode)
      : fs_id_(inode->FsId()),
        ino_(inode->Ino()),
        inode_(inode),
        shard_boundaries_(inode->ShardBoundaries()),
        base_version_(inode->Version()),
        delta_version_(base_version_),
        operation_processor_(operation_processor) {}

  ~ShardPartition() = default;

  static PartitionPtr New(OperationProcessorSPtr operation_processor, InodeSPtr& inode) {
    return std::make_shared<ShardPartition>(operation_processor, inode);
  }

  uint32_t FsId() const { return fs_id_; }
  Ino INo() const { return ino_; }

  uint64_t BaseVersion();
  uint64_t DeltaVersion();

  InodeSPtr ParentInode();
  void SetParentInode(InodeSPtr parent_inode);

  Status Get(const std::string& name, Dentry& out);
  std::vector<Dentry> GetAll();

  Status Scan(const std::string& trace_id, const std::string& start_name, uint32_t limit, bool is_only_dir,
              std::vector<Dentry>& dentries);

  void Put(const Dentry& dentry, uint64_t version);
  void PutWithInode(const Dentry& dentry);
  void Delete(const std::string& name, uint64_t version);
  void Delete(const std::vector<std::string>& names, uint64_t version);

  // delta dentry op too many may cause performance issue, need compact to reduce the op count.
  bool NeedCompact();

  bool Empty() const;
  size_t Size() const;
  size_t ShardSize() const;
  size_t Bytes() const;

  void Dump(Json::Value& value) const;

 private:
  friend class PartitionCache;

  // delta operations
  enum class DentryOpType : uint8_t { ADD = 0, DELETE = 1 };

  struct DentryOp {
    DentryOpType op_type;
    uint64_t version;
    Dentry dentry;
    uint64_t time_s;
  };

  void AddDeltaOpNoLock(DentryOp&& op);
  void ApplyDeltaOpNoLock(DirShardSPtr shard);

  uint64_t NextShardID() { return shard_id_generator_.fetch_add(1, std::memory_order_relaxed); }

  // store operation
  Status RunOperation(Operation* operation);

  // shard operations
  Range QueryRangeNoLock(const std::string& name);
  DirShardSPtr GetShardNoLock(const Range& range);
  DirShardSPtr GetShard(const std::string& name);
  DirShardSPtr GetShard(const std::string& name, Range& out_range);
  void PutShard(DirShardSPtr shard);
  void PutShardNoLock(DirShardSPtr shard);
  void DeleteShard(const std::string& start);
  void DeleteShardNoLock(const std::string& start);

  Status FetchDirShard(const Range& range, DirShardSPtr& out_shard);

  // refresh partition with latest inode
  bool Refresh(InodeSPtr inode);

  // split dir shard
  Status DoSplitDirShard(const Range& range);
  void AsyncSplitDirShard(const Range& range);

  size_t CleanExpired(uint64_t expire_s);

  const uint32_t fs_id_;
  const Ino ino_;

  mutable utils::RWLock lock_;

  InodeWPtr inode_;

  uint64_t base_version_{0};
  uint64_t delta_version_{0};
  std::list<DentryOp> delta_dentry_ops_;

  std::vector<std::string> shard_boundaries_;

  // key: shard range start
  absl::flat_hash_map<std::string, DirShardSPtr> shard_map_;

  // concurrency control flags
  std::atomic<bool> is_compacting_{false};
  std::atomic<bool> is_splitting_{false};

  std::atomic<uint64_t> shard_id_generator_{1};

  OperationProcessorSPtr operation_processor_;
};

class PartitionCache {
 public:
  PartitionCache(uint32_t fs_id);
  ~PartitionCache();

  PartitionCache(const PartitionCache&) = delete;
  PartitionCache& operator=(const PartitionCache&) = delete;
  PartitionCache(PartitionCache&&) = delete;
  PartitionCache& operator=(PartitionCache&&) = delete;

  PartitionPtr PutIf(const PartitionPtr& partition);

  void Delete(Ino ino);
  void DeleteIf(std::function<bool(const Ino&)>&& f);

  void Clear();

  PartitionPtr Get(Ino ino);
  std::vector<PartitionPtr> GetAll();

  size_t Size();
  size_t ShardSize();
  size_t Bytes();

  void CleanExpired(uint64_t expire_s);

  void DescribeByJson(Json::Value& value);
  void Summary(Json::Value& value);

 private:
  using Map = absl::flat_hash_map<Ino, PartitionPtr>;

  const uint32_t fs_id_{0};

  constexpr static size_t kShardNum = 64;
  utils::Shards<Map, kShardNum> shard_map_;

  // metric
  bvar::Adder<int64_t> total_count_;
  bvar::Adder<int64_t> clean_count_;
  bvar::Adder<int64_t> access_miss_count_;
  bvar::Adder<int64_t> access_hit_count_;
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_FILESYSTEM_PARTITION_H_