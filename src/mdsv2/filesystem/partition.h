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

#ifndef DINGOFS_MDV2_FILESYSTEM_PARTITION_H_
#define DINGOFS_MDV2_FILESYSTEM_PARTITION_H_

#include <cstdint>
#include <memory>

#include "json/value.h"
#include "mdsv2/filesystem/dentry.h"
#include "mdsv2/filesystem/inode.h"

namespace dingofs {
namespace mdsv2 {

class Partition;
using PartitionPtr = std::shared_ptr<Partition>;

// compose parent inode and its children dentry
// consider locality
class Partition {
 public:
  Partition(InodeSPtr parent_inode) : parent_inode_(parent_inode) {};
  ~Partition() = default;

  static PartitionPtr New(InodeSPtr parent_inode) { return std::make_shared<Partition>(parent_inode); }

  InodeSPtr ParentInode();

  void PutChild(const Dentry& dentry);
  void DeleteChild(const std::string& name);
  void DeleteChildIf(const std::string& name, Ino ino);

  bool HasChild();
  bool GetChild(const std::string& name, Dentry& dentry);
  std::vector<Dentry> GetChildren(const std::string& start_name, uint32_t limit, bool is_only_dir);
  std::vector<Dentry> GetAllChildren();

 private:
  InodeSPtr parent_inode_;

  utils::RWLock lock_;

  // name -> dentry
  std::map<std::string, Dentry> children_;
};

// use lru cache to store partition
class PartitionCache {
 public:
  PartitionCache(uint32_t fs_id);
  ~PartitionCache();

  PartitionCache(const PartitionCache&) = delete;
  PartitionCache& operator=(const PartitionCache&) = delete;
  PartitionCache(PartitionCache&&) = delete;
  PartitionCache& operator=(PartitionCache&&) = delete;

  void Put(Ino ino, PartitionPtr partition);
  void Delete(Ino ino);
  void BatchDeleteInodeIf(const std::function<bool(const Ino&)>& f);
  void Clear();

  PartitionPtr Get(Ino ino);

  std::map<uint64_t, PartitionPtr> GetAll();

  void DescribeByJson(Json::Value& value);

 private:
  uint32_t fs_id_{0};
  // dir ino -> partition
  utils::LRUCache<uint64_t, PartitionPtr> cache_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_FILESYSTEM_PARTITION_H_