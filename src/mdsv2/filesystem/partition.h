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

#include <memory>

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
  Partition(InodePtr parent_inode) : parent_inode_(parent_inode){};
  ~Partition() = default;

  static PartitionPtr New(InodePtr parent_inode) { return std::make_shared<Partition>(parent_inode); }

  InodePtr ParentInode();

  void PutChild(const Dentry& dentry);
  void DeleteChild(const std::string& name);

  bool HasChild();
  bool GetChild(const std::string& name, Dentry& dentry);
  std::vector<Dentry> GetChildren(const std::string& start_name, uint32_t limit, bool is_only_dir);
  std::vector<Dentry> GetAllChildren();

 private:
  InodePtr parent_inode_;

  utils::RWLock lock_;

  std::map<std::string, Dentry> children_;
};

// use lru cache to store partition
class PartitionCache {
 public:
  PartitionCache();
  ~PartitionCache();

  void Put(uint64_t ino, PartitionPtr partition);
  void Delete(uint64_t ino);

  PartitionPtr Get(uint64_t ino);

 private:
  utils::LRUCache<uint64_t, PartitionPtr> cache_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_FILESYSTEM_PARTITION_H_