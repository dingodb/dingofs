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

#include "mdsv2/filesystem/inode.h"

#include <glog/logging.h>

#include <utility>

#include "fmt/core.h"
#include "mdsv2/common/constant.h"
#include "mdsv2/common/logging.h"

namespace dingofs {
namespace mdsv2 {

Inode::Inode(uint32_t fs_id, uint64_t ino) : fs_id_(fs_id), ino_(ino) {}

Inode::Inode(const pb::mdsv2::Inode& inode)
    : fs_id_(inode.fs_id()),
      ino_(inode.ino()),
      length_(inode.length()),
      ctime_(inode.ctime()),
      mtime_(inode.mtime()),
      atime_(inode.atime()),
      uid_(inode.uid()),
      gid_(inode.gid()),
      mode_(inode.mode()),
      nlink_(inode.nlink()),
      type_(inode.type()),
      symlink_(inode.symlink()),
      rdev_(inode.rdev()),
      dtime_(inode.dtime()),
      openmpcount_(inode.openmpcount()) {
  for (auto parent : inode.parent_inos()) {
    parents_.push_back(parent);
  }

  for (const auto& [index, slice_list] : inode.chunks()) {
    chunks_.insert(std::make_pair(index, slice_list));
  }
  for (const auto& [key, value] : inode.xattrs()) {
    xattrs_.insert(std::make_pair(key, value));
  }
}

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
  chunks_ = inode.chunks_;
  xattrs_ = inode.xattrs_;
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
  chunks_ = inode.chunks_;
  xattrs_ = inode.xattrs_;

  return *this;
}

Inode::~Inode() {}  // NOLINT

uint64_t Inode::Length() {
  utils::ReadLockGuard lk(lock_);

  return length_;
}

void Inode::SetLength(uint64_t length) {
  utils::WriteLockGuard lk(lock_);

  length_ = length;
}

uint64_t Inode::Ctime() {
  utils::ReadLockGuard lk(lock_);

  return ctime_;
}

void Inode::SetCtime(uint64_t ctime) {
  utils::WriteLockGuard lk(lock_);

  ctime_ = ctime;
}

uint64_t Inode::Mtime() {
  utils::ReadLockGuard lk(lock_);

  return mtime_;
}

void Inode::SetMtime(uint64_t mtime) {
  utils::WriteLockGuard lk(lock_);

  mtime_ = mtime;
}

uint64_t Inode::Atime() {
  utils::ReadLockGuard lk(lock_);

  return atime_;
}

void Inode::SetAtime(uint64_t atime) {
  utils::WriteLockGuard lk(lock_);

  atime_ = atime;
}

uint32_t Inode::Uid() {
  utils::ReadLockGuard lk(lock_);

  return uid_;
}

void Inode::SetUid(uint32_t uid) {
  utils::WriteLockGuard lk(lock_);

  uid_ = uid;
}

uint32_t Inode::Gid() {
  utils::ReadLockGuard lk(lock_);

  return gid_;
}

void Inode::SetGid(uint32_t gid) {
  utils::WriteLockGuard lk(lock_);

  gid_ = gid;
}

uint32_t Inode::Mode() {
  utils::ReadLockGuard lk(lock_);

  return mode_;
}

void Inode::SetMode(uint32_t mode) {
  utils::WriteLockGuard lk(lock_);

  mode_ = mode;
}

uint32_t Inode::Nlink() {
  utils::ReadLockGuard lk(lock_);

  return nlink_;
}

void Inode::SetNlink(uint32_t nlink) {
  utils::WriteLockGuard lk(lock_);

  nlink_ = nlink;
}

void Inode::SetNlinkDelta(int32_t delta, uint64_t time) {
  utils::WriteLockGuard lk(lock_);

  nlink_ += delta;
  ctime_ = time;
  mtime_ = time;
}

void Inode::PrepareIncNlink() {
  utils::WriteLockGuard lk(lock_);

  ++pending_nlink_;
}

void Inode::CommitIncNlink(uint64_t time) {
  utils::WriteLockGuard lk(lock_);
  --pending_nlink_;
  ++nlink_;
  ctime_ = time;
  mtime_ = time;
}

void Inode::PrepareDecNlink() {
  utils::WriteLockGuard lk(lock_);

  --pending_nlink_;
}

void Inode::CommitDecNlink(uint64_t time) {
  utils::WriteLockGuard lk(lock_);

  ++pending_nlink_;
  --nlink_;
  ctime_ = time;
  mtime_ = time;
}

pb::mdsv2::FileType Inode::Type() {
  utils::ReadLockGuard lk(lock_);

  return type_;
}

void Inode::SetType(pb::mdsv2::FileType type) {
  utils::WriteLockGuard lk(lock_);

  type_ = type;
}

const std::string& Inode::Symlink() {
  utils::ReadLockGuard lk(lock_);

  return symlink_;
}

void Inode::SetSymlink(const std::string& symlink) {
  utils::WriteLockGuard lk(lock_);

  symlink_ = symlink;
}

uint64_t Inode::Rdev() {
  utils::ReadLockGuard lk(lock_);

  return rdev_;
}

void Inode::SetRdev(uint64_t rdev) {
  utils::WriteLockGuard lk(lock_);

  rdev_ = rdev;
}

uint32_t Inode::Dtime() {
  utils::ReadLockGuard lk(lock_);

  return dtime_;
}

void Inode::SetDtime(uint32_t dtime) {
  utils::WriteLockGuard lk(lock_);

  dtime_ = dtime;
}

uint32_t Inode::Openmpcount() {
  utils::ReadLockGuard lk(lock_);

  return openmpcount_;
}

void Inode::SetOpenmpcount(uint32_t openmpcount) {
  utils::WriteLockGuard lk(lock_);

  openmpcount_ = openmpcount;
}

Inode::ChunkMap Inode::GetChunkMap() {
  utils::ReadLockGuard lk(lock_);

  return chunks_;
}

pb::mdsv2::SliceList Inode::GetChunk(uint64_t chunk_index) {
  utils::ReadLockGuard lk(lock_);

  auto it = chunks_.find(chunk_index);
  if (it == chunks_.end()) {
    return pb::mdsv2::SliceList();
  }

  return it->second;
}

void Inode::AppendChunk(uint64_t chunk_index, const pb::mdsv2::SliceList& slice_list) {
  utils::WriteLockGuard lk(lock_);

  auto it = chunks_.find(chunk_index);
  if (it == chunks_.end()) {
    chunks_.insert({chunk_index, slice_list});
  } else {
    it->second.MergeFrom(slice_list);
  }
}

Inode::XAttrMap Inode::GetXAttrMap() {
  utils::ReadLockGuard lk(lock_);

  return xattrs_;
}

std::string Inode::GetXAttr(const std::string& name) {
  utils::ReadLockGuard lk(lock_);

  auto it = xattrs_.find(name);

  return it != xattrs_.end() ? it->second : "";
}

void Inode::SetXAttr(const std::string& name, const std::string& value) {
  utils::WriteLockGuard lk(lock_);

  xattrs_[name] = value;
}

void Inode::SetXAttr(const std::map<std::string, std::string>& xattr) {
  utils::WriteLockGuard lk(lock_);

  for (const auto& [key, value] : xattr) {
    xattrs_[key] = value;
  }
}

void Inode::SetAttr(const pb::mdsv2::Inode& inode, uint32_t to_set) {
  utils::WriteLockGuard lk(lock_);

  if (to_set & kSetAttrMode) {
    mode_ = inode.mode();
  } else if (to_set & kSetAttrUid) {
    uid_ = inode.uid();
  } else if (to_set & kSetAttrGid) {
    gid_ = inode.gid();
  } else if (to_set & kSetAttrLength) {
    length_ = inode.length();
  } else if (to_set & kSetAttrAtime) {
    atime_ = inode.atime();
  } else if (to_set & kSetAttrMtime) {
    mtime_ = inode.mtime();
  } else if (to_set & kSetAttrCtime) {
    ctime_ = inode.ctime();
  } else if (to_set & kSetAttrNlink) {
    nlink_ = inode.nlink();
  }
}

void Inode::AddParent(uint64_t parent_ino) {
  utils::WriteLockGuard lk(lock_);

  parents_.push_back(parent_ino);
}

pb::mdsv2::Inode Inode::CopyTo() {
  pb::mdsv2::Inode inode;
  CopyTo(inode);
  return std::move(inode);
}

void Inode::CopyTo(pb::mdsv2::Inode& inode) {
  inode.set_fs_id(fs_id_);
  inode.set_ino(ino_);
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

  for (auto& parent : parents_) {
    inode.add_parent_inos(parent);
  }

  for (const auto& [index, slice_list] : chunks_) {
    inode.mutable_chunks()->insert({index, slice_list});
  }

  for (const auto& [key, value] : xattrs_) {
    inode.mutable_xattrs()->insert({key, value});
  }
}

InodeCache::InodeCache() : cache_(0) {}  // NOLINT

InodeCache::~InodeCache() {}  // NOLINT

void InodeCache::PutInode(uint64_t ino, InodePtr inode) { cache_.Put(ino, inode); }

void InodeCache::DeleteInode(uint64_t ino) { cache_.Remove(ino); };

InodePtr InodeCache::GetInode(uint64_t ino) {
  InodePtr inode;
  if (!cache_.Get(ino, &inode)) {
    DINGO_LOG(INFO) << fmt::format("inode({}) not found.", ino);
    return nullptr;
  }

  return inode;
}

std::vector<InodePtr> InodeCache::GetInodes(std::vector<uint64_t> inoes) {
  std::vector<InodePtr> inodes;
  for (auto& ino : inoes) {
    InodePtr inode;
    if (cache_.Get(ino, &inode)) {
      inodes.push_back(inode);
    }
  }

  return inodes;
}

}  // namespace mdsv2
}  // namespace dingofs