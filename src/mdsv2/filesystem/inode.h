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

#ifndef DINGOFS_MDV2_FILESYSTEM_INODE_H_
#define DINGOFS_MDV2_FILESYSTEM_INODE_H_

#include <sys/types.h>

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "mdsv2/common/type.h"
#include "utils/concurrent/concurrent.h"
#include "utils/lru_cache.h"

namespace dingofs {
namespace mdsv2 {

class Inode;
using InodeSPtr = std::shared_ptr<Inode>;
using InodeWPtr = std::weak_ptr<Inode>;

class Inode {
 public:
  using AttrEntry = mdsv2::AttrEntry;
  using XAttrMap = ::google::protobuf::Map<std::string, std::string>;
  using ChunkMap = ::google::protobuf::Map<uint64_t, ChunkEntry>;

  Inode(const AttrEntry& attr) { attr_ = attr; }
  Inode(AttrEntry&& attr) { attr_ = std::move(attr); }
  ~Inode() = default;

  static InodeSPtr New(const AttrEntry& inode) { return std::make_shared<Inode>(inode); }

  uint32_t FsId();
  uint64_t Ino();
  FileType Type();
  uint64_t Length();
  uint32_t Uid();
  uint32_t Gid();
  uint32_t Mode();
  uint32_t Nlink();
  std::string Symlink();
  uint64_t Rdev();
  uint32_t Dtime();
  uint64_t Ctime();
  uint64_t Mtime();
  uint64_t Atime();
  uint32_t Openmpcount();
  uint64_t Version();

  XAttrMap XAttrs();
  std::string XAttr(const std::string& name);

  bool UpdateIf(const AttrEntry& attr);
  bool UpdateIf(AttrEntry&& attr);

  AttrEntry Copy();
  AttrEntry&& Move();

 private:
  utils::RWLock lock_;

  AttrEntry attr_;
};

// cache all file/dir inode
class InodeCache {
 public:
  InodeCache(uint32_t fs_id);
  ~InodeCache();

  InodeCache(const InodeCache&) = delete;
  InodeCache& operator=(const InodeCache&) = delete;
  InodeCache(InodeCache&&) = delete;
  InodeCache& operator=(InodeCache&&) = delete;

  void PutInode(Ino ino, InodeSPtr inode);
  void DeleteInode(Ino ino);
  void BatchDeleteInodeIf(const std::function<bool(const Ino&)>& f);
  void Clear();

  InodeSPtr GetInode(Ino ino);
  std::vector<InodeSPtr> GetInodes(std::vector<uint64_t> inoes);
  std::map<uint64_t, InodeSPtr> GetAllInodes();

 private:
  uint32_t fs_id_{0};
  // ino -> inode
  utils::LRUCache<uint64_t, InodeSPtr> cache_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_FILESYSTEM_INODE_H_