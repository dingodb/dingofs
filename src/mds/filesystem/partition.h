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

#include <cstdint>
#include <memory>

#include "json/value.h"
#include "mds/filesystem/dentry.h"
#include "mds/filesystem/inode.h"

namespace dingofs {
namespace mds {

class Partition;
using PartitionPtr = std::shared_ptr<Partition>;

// compose parent inode and its children dentry
// consider locality
class Partition {
 public:
  Partition(InodeSPtr inode) : ino_(inode->Ino()), inode_(inode), version_(inode->Version()) {};
  ~Partition() = default;

  static PartitionPtr New(InodeSPtr inode) { return std::make_shared<Partition>(inode); }

  Ino INo() const { return ino_; }

  uint64_t Version();
  InodeSPtr ParentInode();
  void SetParentInode(InodeSPtr parent_inode);

  void PutChild(const Dentry& dentry, uint64_t version = 0);
  void DeleteChild(const std::string& name, uint64_t version = 0);
  void DeleteChildIf(const std::string& name, Ino ino, uint64_t version = 0);

  bool HasChild();
  bool GetChild(const std::string& name, Dentry& dentry);
  std::vector<Dentry> GetChildren(const std::string& start_name, uint32_t limit, bool is_only_dir);
  std::vector<Dentry> GetAllChildren();

 private:
  const Ino ino_;

  utils::RWLock lock_;

  InodeWPtr inode_;
  uint64_t version_{0};

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

  void PutIf(Ino ino, PartitionPtr partition);
  void Delete(Ino ino);
  void DeleteIf(Ino ino, uint64_t version);
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

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_FILESYSTEM_PARTITION_H_