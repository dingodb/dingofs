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
  Inode(uint32_t fs_id, uint64_t ino);
  Inode(const pb::mdsv2::Inode& inode);
  ~Inode();

  Inode(const Inode& inode);
  Inode& operator=(const Inode& inode);

  static InodePtr New(uint32_t fs_id, uint64_t ino) { return std::make_shared<Inode>(fs_id, ino); }
  static InodePtr New(const pb::mdsv2::Inode& inode) { return std::make_shared<Inode>(inode); }

  using S3ChunkMap = std::map<uint64_t, pb::mdsv2::S3ChunkList>;
  using XAttrMap = std::map<std::string, std::string>;

  uint32_t FsId() const { return fs_id_; }
  uint64_t Ino() const { return ino_; }

  uint64_t Length();
  void SetLength(uint64_t length);

  uint64_t Ctime();
  void SetCtime(uint64_t ctime);

  uint64_t Mtime();
  void SetMtime(uint64_t mtime);

  uint64_t Atime();
  void SetAtime(uint64_t atime);

  uint32_t Uid();
  void SetUid(uint32_t uid);

  uint32_t Gid();
  void SetGid(uint32_t gid);

  uint32_t Mode();
  void SetMode(uint32_t mode);

  uint32_t Nlink();
  void SetNlink(uint32_t nlink);
  // void SetNlink(uint32_t nlink, uint64_t time);
  void SetNlinkDelta(int32_t delta, uint64_t time);

  pb::mdsv2::FileType Type();
  void SetType(pb::mdsv2::FileType type);

  const std::string& Symlink();
  void SetSymlink(const std::string& symlink);

  uint64_t Rdev();
  void SetRdev(uint64_t rdev);

  uint32_t Dtime();
  void SetDtime(uint32_t dtime);

  uint32_t Openmpcount();
  void SetOpenmpcount(uint32_t openmpcount);

  S3ChunkMap GetS3ChunkMap();

  XAttrMap GetXAttrMap();
  std::string GetXAttr(const std::string& name);
  void SetXAttr(const std::string& name, const std::string& value);
  void SetXAttr(const std::map<std::string, std::string>& xattr);

  void SetAttr(const pb::mdsv2::Inode& inode, uint32_t to_set);

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
  pb::mdsv2::FileType type_{0};
  std::string symlink_;
  uint64_t rdev_{0};
  uint32_t dtime_{0};
  uint32_t openmpcount_{0};

  S3ChunkMap s3_chunk_map_;
  XAttrMap xattr_map_;
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

 private:
  utils::LRUCache<uint64_t, InodePtr> cache_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_FILESYSTEM_INODE_H_