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
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"

namespace dingofs {
namespace mdsv2 {

Inode::Inode(uint32_t fs_id, uint64_t ino, pb::mdsv2::FileType type, uint32_t mode, uint32_t gid, uint32_t uid,
             uint32_t nlink)
    : fs_id_(fs_id), ino_(ino), type_(type), mode_(mode), gid_(gid), uid_(uid), nlink_(nlink) {
  uint64_t now_ns = Helper::TimestampNs();
  ctime_ = now_ns;
  mtime_ = now_ns;
  atime_ = now_ns;
}

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
  xattrs_ = inode.xattrs_;

  return *this;
}

Inode::~Inode() {}  // NOLINT

uint64_t Inode::Length() {
  utils::ReadLockGuard lk(lock_);

  return length_;
}

uint32_t Inode::Uid() {
  utils::ReadLockGuard lk(lock_);

  return uid_;
}

uint32_t Inode::Gid() {
  utils::ReadLockGuard lk(lock_);

  return gid_;
}

uint32_t Inode::Mode() {
  utils::ReadLockGuard lk(lock_);

  return mode_;
}

uint32_t Inode::Nlink() {
  utils::ReadLockGuard lk(lock_);

  return nlink_;
}

const std::string& Inode::Symlink() {
  utils::ReadLockGuard lk(lock_);

  return symlink_;
}

uint64_t Inode::Rdev() {
  utils::ReadLockGuard lk(lock_);

  return rdev_;
}

uint32_t Inode::Dtime() {
  utils::ReadLockGuard lk(lock_);

  return dtime_;
}
uint64_t Inode::Ctime() {
  utils::ReadLockGuard lk(lock_);

  return ctime_;
}

uint64_t Inode::Mtime() {
  utils::ReadLockGuard lk(lock_);

  return mtime_;
}

uint64_t Inode::Atime() {
  utils::ReadLockGuard lk(lock_);

  return atime_;
}

uint32_t Inode::Openmpcount() {
  utils::ReadLockGuard lk(lock_);

  return openmpcount_;
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

bool Inode::UpdateNlink(uint64_t version, uint32_t nlink, uint64_t time_ns) {
  utils::WriteLockGuard lk(lock_);

  if (version <= version_) {
    return false;
  }

  nlink_ = nlink;
  version_ = version;
  ctime_ = time_ns;
  mtime_ = time_ns;

  return true;
}

bool Inode::UpdateAttr(uint64_t version, const pb::mdsv2::Inode& inode, uint32_t to_set) {
  utils::WriteLockGuard lk(lock_);

  if (version <= version_) {
    return false;
  }

  if (to_set & kSetAttrMode) {
    mode_ = inode.mode();
  }

  if (to_set & kSetAttrUid) {
    uid_ = inode.uid();
  }

  if (to_set & kSetAttrGid) {
    gid_ = inode.gid();
  }

  if (to_set & kSetAttrLength) {
    length_ = inode.length();
  }

  if (to_set & kSetAttrAtime) {
    atime_ = inode.atime();
  }

  if (to_set & kSetAttrMtime) {
    mtime_ = inode.mtime();
  }

  if (to_set & kSetAttrCtime) {
    ctime_ = inode.ctime();
  }

  if (to_set & kSetAttrNlink) {
    nlink_ = inode.nlink();
  }

  return true;
}

bool Inode::UpdateXAttr(uint64_t version, const std::string& name, const std::string& value) {
  utils::WriteLockGuard lk(lock_);

  if (version <= version_) {
    return false;
  }

  xattrs_[name] = value;

  return true;
}

bool Inode::UpdateXAttr(uint64_t version, const std::map<std::string, std::string>& xattrs) {
  utils::WriteLockGuard lk(lock_);

  if (version <= version_) {
    return false;
  }

  for (const auto& [key, value] : xattrs) {
    xattrs_[key] = value;
  }

  return true;
}

bool Inode::UpdateParent(uint64_t version, uint64_t parent_ino) {
  utils::WriteLockGuard lk(lock_);

  if (version <= version_) {
    return false;
  }

  parents_.push_back(parent_ino);

  return true;
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

std::map<uint64_t, InodePtr> InodeCache::GetAllInodes() { return cache_.GetAll(); }

}  // namespace mdsv2
}  // namespace dingofs