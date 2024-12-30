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

#include "dingofs/src/mdsv2/filesystem/inode.h"

#include "bthread/mutex.h"

namespace dingofs {
namespace mdsv2 {

Inode::Inode(uint32_t fs_id, uint64_t ino) : fs_id_(fs_id), ino_(ino) { bthread_mutex_init(&mutex_, nullptr); }

Inode::~Inode() { bthread_mutex_destroy(&mutex_); }

Inode::Inode(const Inode& inode) {
  fs_id_ = inode.fs_id_;
  ino_ = inode.ino_;
  length_ = inode.length_;
  ctime_ = inode.ctime_;
  mtime_ = inode.mtime_;
  atime_ = inode.atime_;
  uid_ = inode.uid_;
  gid_ = inode.gid_;
  mode_ = inode.mode_;
  nlink_ = inode.nlink_;
  type_ = inode.type_;
  symlink_ = inode.symlink_;
  rdev_ = inode.rdev_;
  dtime_ = inode.dtime_;
  openmpcount_ = inode.openmpcount_;
  s3_chunk_map_ = inode.s3_chunk_map_;
  xattr_map_ = inode.xattr_map_;
}

Inode& Inode::operator=(const Inode& inode) {
  if (this == &inode) {
    return *this;
  }

  fs_id_ = inode.fs_id_;
  ino_ = inode.ino_;
  length_ = inode.length_;
  ctime_ = inode.ctime_;
  mtime_ = inode.mtime_;
  atime_ = inode.atime_;
  uid_ = inode.uid_;
  gid_ = inode.gid_;
  mode_ = inode.mode_;
  nlink_ = inode.nlink_;
  type_ = inode.type_;
  symlink_ = inode.symlink_;
  rdev_ = inode.rdev_;
  dtime_ = inode.dtime_;
  openmpcount_ = inode.openmpcount_;
  s3_chunk_map_ = inode.s3_chunk_map_;
  xattr_map_ = inode.xattr_map_;

  return *this;
}

std::string Inode::SerializeAsString() {
  pb::mdsv2::Inode inode;
  inode.set_fs_id(fs_id_);
  inode.set_inode_id(ino_);
  inode.set_length(length_);
  inode.set_ctime(ctime_);
  inode.set_mtime(mtime_);
  inode.set_atime(atime_);
  inode.set_uid(uid_);
  inode.set_gid(gid_);
  inode.set_mode(mode_);
  inode.set_nlink(nlink_);
  inode.set_type(type_);
  inode.set_symlink(symlink_);
  inode.set_rdev(rdev_);
  inode.set_dtime(dtime_);
  inode.set_openmpcount(openmpcount_);

  return inode.SerializeAsString();
}

std::string Inode::GetXAttr(const std::string& name) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = xattr_map_.find(name);

  return it != xattr_map_.end() ? it->second : "";
}

InodeMap::InodeMap() { bthread_mutex_init(&mutex_, nullptr); }

InodeMap::~InodeMap() { bthread_mutex_destroy(&mutex_); }

void InodeMap::AddInode(uint64_t ino, InodePtr inode) {
  BAIDU_SCOPED_LOCK(mutex_);

  inode_map_[ino] = inode;
}
void InodeMap::DeleteInode(uint64_t ino) {
  BAIDU_SCOPED_LOCK(mutex_);

  inode_map_.erase(ino);
};

InodePtr InodeMap::GetInode(uint64_t ino) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = inode_map_.find(ino);
  if (it == inode_map_.end()) {
    return nullptr;
  }

  return it->second;
}

std::vector<InodePtr> InodeMap::GetInodes(std::vector<uint64_t> ino) {
  BAIDU_SCOPED_LOCK(mutex_);

  std::vector<InodePtr> inodes;
  for (auto& i : ino) {
    auto it = inode_map_.find(i);
    if (it != inode_map_.end()) {
      inodes.push_back(it->second);
    }
  }

  return inodes;
}

}  // namespace mdsv2
}  // namespace dingofs