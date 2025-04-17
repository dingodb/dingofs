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

#include "dingofs/mdsv2.pb.h"
#include "utils/concurrent/concurrent.h"
#include "utils/lru_cache.h"

namespace dingofs {
namespace mdsv2 {

class Inode;
using InodePtr = std::shared_ptr<Inode>;

class Inode {
 public:
  Inode() = default;
  Inode(uint32_t fs_id, uint64_t ino, pb::mdsv2::FileType type, uint32_t mode, uint32_t gid, uint32_t uid,
        uint32_t nlink);
  Inode(const pb::mdsv2::Inode& inode);
  ~Inode();

  Inode(const Inode& inode);
  Inode& operator=(const Inode& inode);

  static InodePtr New(uint32_t fs_id, uint64_t ino, pb::mdsv2::FileType type, uint32_t mode, uint32_t gid, uint32_t uid,
                      uint32_t nlink) {
    return std::make_shared<Inode>(fs_id, ino, type, mode, gid, uid, nlink);
  }
  static InodePtr New(const pb::mdsv2::Inode& inode) { return std::make_shared<Inode>(inode); }

  using XAttrMap = std::map<std::string, std::string>;

  uint32_t FsId() const { return fs_id_; }
  uint64_t Ino() const { return ino_; }
  pb::mdsv2::FileType Type() { return type_; }
  uint64_t Length();
  uint32_t Uid();
  uint32_t Gid();
  uint32_t Mode();
  uint32_t Nlink();
  const std::string& Symlink();
  uint64_t Rdev();
  uint32_t Dtime();
  uint64_t Ctime();
  uint64_t Mtime();
  uint64_t Atime();
  uint32_t Openmpcount();
  uint64_t Version() const { return version_; }

  XAttrMap GetXAttrMap();
  std::string GetXAttr(const std::string& name);

  bool UpdateNlink(uint64_t version, uint32_t nlink, uint64_t time_ns);
  bool UpdateAttr(uint64_t version, const pb::mdsv2::Inode& inode, uint32_t to_set);

  bool UpdateXAttr(uint64_t version, const std::string& name, const std::string& value);
  bool UpdateXAttr(uint64_t version, const std::map<std::string, std::string>& xattrs);

  bool UpdateChunk(uint64_t version, uint64_t chunk_index, const pb::mdsv2::SliceList& slice_list);

  bool UpdateParent(uint64_t version, uint64_t parent_ino);

  pb::mdsv2::Inode CopyTo();
  void CopyTo(pb::mdsv2::Inode& inode);

 private:
  utils::RWLock lock_;

  uint32_t fs_id_{0};
  uint64_t ino_{0};
  uint64_t length_{0};
  uint64_t ctime_{0};
  uint64_t mtime_{0};
  uint64_t atime_{0};
  uint32_t uid_{0};
  uint32_t gid_{0};
  uint32_t mode_{0};
  int32_t nlink_{0};
  int32_t pending_nlink_{0};
  pb::mdsv2::FileType type_{0};
  std::string symlink_;
  uint64_t rdev_{0};
  uint32_t dtime_{0};
  uint32_t openmpcount_{0};
  std::vector<uint64_t> parents_;

  XAttrMap xattrs_;

  uint64_t version_{0};
};

// cache all file/dir inode
class InodeCache {
 public:
  InodeCache();
  ~InodeCache();

  InodeCache(const InodeCache&) = delete;
  InodeCache& operator=(const InodeCache&) = delete;

  void PutInode(uint64_t ino, InodePtr inode);
  void DeleteInode(uint64_t ino);

  InodePtr GetInode(uint64_t ino);
  std::vector<InodePtr> GetInodes(std::vector<uint64_t> inoes);
  std::map<uint64_t, InodePtr> GetAllInodes();

 private:
  utils::LRUCache<uint64_t, InodePtr> cache_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_FILESYSTEM_INODE_H_