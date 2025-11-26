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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "absl/container/btree_map.h"
#include "json/value.h"
#include "mds/filesystem/dentry.h"
#include "mds/filesystem/inode.h"

namespace dingofs {
namespace mds {

class Partition;
using PartitionPtr = std::shared_ptr<Partition>;

class Partition {
 public:
  Partition(InodeSPtr inode) : ino_(inode->Ino()), inode_(inode), base_version_(inode->Version()) {};
  Partition(Partition&& partition) noexcept : ino_(partition.ino_) {
    inode_ = partition.inode_;
    base_version_ = partition.base_version_;
    delta_version_ = partition.delta_version_;
    children_.swap(partition.children_);
    delta_dentry_ops_.swap(partition.delta_dentry_ops_);
  }

  ~Partition() = default;

  static PartitionPtr New(InodeSPtr& inode) { return std::make_shared<Partition>(inode); }
  static PartitionPtr New(Partition&& partition) { return std::make_shared<Partition>(std::move(partition)); }

  Ino INo() const { return ino_; }

  uint64_t BaseVersion();
  uint64_t DeltaVersion();

  InodeSPtr ParentInode();
  void SetParentInode(InodeSPtr parent_inode);

  void Put(const Dentry& dentry, uint64_t version);
  void PutWithInode(const Dentry& dentry);

  void Delete(const std::string& name, uint64_t version);

  bool Empty();
  size_t Size();

  bool Get(const std::string& name, Dentry& dentry);
  std::vector<Dentry> Scan(const std::string& start_name, uint32_t limit, bool is_only_dir);
  std::vector<Dentry> GetAll();

  bool Merge(PartitionPtr& other_partition);
  bool Merge(Partition&& other_partition);

  void UpdateLastAccessTime();
  uint64_t LastAccessTimeS();

 private:
  const Ino ino_;

  utils::RWLock lock_;

  uint64_t base_version_{0};
  uint64_t delta_version_{0};

  InodeWPtr inode_;

  // name -> dentry
  absl::btree_map<std::string, Dentry> children_;

  // version -> dentry
  enum class DentryOpType : uint8_t { ADD = 0, DELETE = 1 };

  struct DentryOp {
    DentryOpType op_type;
    uint64_t version;
    Dentry dentry;
    uint64_t time_s;
  };
  void AddDeltaDentryOp(DentryOp&& op);

  std::list<DentryOp> delta_dentry_ops_;

  std::atomic<uint64_t> last_access_time_s_{0};
};

class PartitionCache {
 public:
  PartitionCache(uint32_t fs_id);
  ~PartitionCache();

  PartitionCache(const PartitionCache&) = delete;
  PartitionCache& operator=(const PartitionCache&) = delete;
  PartitionCache(PartitionCache&&) = delete;
  PartitionCache& operator=(PartitionCache&&) = delete;

  PartitionPtr PutIf(PartitionPtr& partition);
  PartitionPtr PutIf(Partition&& partition);

  void Delete(Ino ino);
  void DeleteIf(std::function<bool(const Ino&)>&& f);

  void Clear();

  PartitionPtr Get(Ino ino);
  std::vector<PartitionPtr> GetAll();

  size_t Size();

  void CleanExpired(uint64_t expire_s);

  void DescribeByJson(Json::Value& value);

 private:
  using Map = absl::flat_hash_map<Ino, PartitionPtr>;

  const uint32_t fs_id_{0};

  constexpr static size_t kShardNum = 64;
  utils::Shards<Map, kShardNum> shard_map_;

  // metric
  bvar::Adder<int64_t> access_miss_count_;
  bvar::Adder<int64_t> access_hit_count_;
  bvar::Adder<int64_t> clean_count_;
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_FILESYSTEM_PARTITION_H_