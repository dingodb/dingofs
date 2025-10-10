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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_V2_INODE_CACHE_H_
#define DINGOFS_SRC_CLIENT_VFS_META_V2_INODE_CACHE_H_

#include <sys/types.h>

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "json/value.h"
#include "mds/common/type.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

class Inode;
using InodeSPtr = std::shared_ptr<Inode>;

using mds::ChunkEntry;
using mds::Ino;

class Inode {
 public:
  using FileType = mds::FileType;
  using AttrEntry = mds::AttrEntry;
  using XAttrMap = ::google::protobuf::Map<std::string, std::string>;
  using ChunkMap = ::google::protobuf::Map<uint64_t, ChunkEntry>;

  Inode(const AttrEntry& attr) { attr_ = attr; }
  Inode(AttrEntry&& attr) { attr_ = std::move(attr); }
  ~Inode() = default;

  static InodeSPtr New(const AttrEntry& inode) {
    return std::make_shared<Inode>(inode);
  }

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

  void ExpandLength(uint64_t length);

  AttrEntry Copy();
  AttrEntry&& Move();

 private:
  utils::RWLock lock_;

  AttrEntry attr_;
};

class InodeCache;
using InodeCacheSPtr = std::shared_ptr<InodeCache>;

// cache all file/dir inode
class InodeCache {
 public:
  using AttrEntry = mds::AttrEntry;

  InodeCache(uint32_t fs_id);
  ~InodeCache();

  InodeCache(const InodeCache&) = delete;
  InodeCache& operator=(const InodeCache&) = delete;
  InodeCache(InodeCache&&) = delete;
  InodeCache& operator=(InodeCache&&) = delete;

  static InodeCacheSPtr New(uint32_t fs_id) {
    return std::make_shared<InodeCache>(fs_id);
  }

  void UpsertInode(Ino ino, const AttrEntry& attr_entry);
  void DeleteInode(Ino ino);
  void Clear();

  InodeSPtr GetInode(Ino ino);
  std::vector<InodeSPtr> GetInodes(std::vector<uint64_t> inoes);
  bool Dump(Json::Value& value);

 private:
  uint32_t fs_id_{0};

  utils::RWLock lock_;
  // ino -> inode
  std::unordered_map<Ino, InodeSPtr> cache_;
};

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_V2_INODE_CACHE_H_