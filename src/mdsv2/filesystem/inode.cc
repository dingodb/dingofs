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

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "fmt/core.h"
#include "mdsv2/common/constant.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace mdsv2 {

static const std::string kInodeCacheMetricsPrefix = "dingofs_inode_cache_";

// 0: no limit
DEFINE_uint32(inode_cache_max_count, 0, "inode cache max count");

uint32_t Inode::FsId() {
  utils::ReadLockGuard lk(lock_);

  return attr_.fs_id();
}

uint64_t Inode::Ino() {
  utils::ReadLockGuard lk(lock_);

  return attr_.ino();
}

pb::mdsv2::FileType Inode::Type() {
  utils::ReadLockGuard lk(lock_);

  return attr_.type();
}

uint64_t Inode::Length() {
  utils::ReadLockGuard lk(lock_);

  return attr_.length();
}

uint32_t Inode::Uid() {
  utils::ReadLockGuard lk(lock_);

  return attr_.uid();
}

uint32_t Inode::Gid() {
  utils::ReadLockGuard lk(lock_);

  return attr_.gid();
}

uint32_t Inode::Mode() {
  utils::ReadLockGuard lk(lock_);

  return attr_.mode();
}

uint32_t Inode::Nlink() {
  utils::ReadLockGuard lk(lock_);

  return attr_.nlink();
}

std::string Inode::Symlink() {
  utils::ReadLockGuard lk(lock_);

  return attr_.symlink();
}

uint64_t Inode::Rdev() {
  utils::ReadLockGuard lk(lock_);

  return attr_.rdev();
}

uint32_t Inode::Dtime() {
  utils::ReadLockGuard lk(lock_);

  return attr_.dtime();
}

uint64_t Inode::Ctime() {
  utils::ReadLockGuard lk(lock_);

  return attr_.ctime();
}

uint64_t Inode::Mtime() {
  utils::ReadLockGuard lk(lock_);

  return attr_.mtime();
}

uint64_t Inode::Atime() {
  utils::ReadLockGuard lk(lock_);

  return attr_.atime();
}

uint32_t Inode::Openmpcount() {
  utils::ReadLockGuard lk(lock_);

  return attr_.openmpcount();
}

uint64_t Inode::Version() {
  utils::ReadLockGuard lk(lock_);

  return attr_.version();
}

Inode::XAttrMap Inode::XAttrs() {
  utils::ReadLockGuard lk(lock_);

  return attr_.xattrs();
}

std::string Inode::XAttr(const std::string& name) {
  utils::ReadLockGuard lk(lock_);

  auto it = attr_.xattrs().find(name);
  return (it != attr_.xattrs().end()) ? it->second : std::string();
}

Inode::ChunkMap Inode::Chunks() {
  utils::ReadLockGuard lk(lock_);

  return attr_.chunks();
}

bool Inode::Chunk(uint64_t index, ChunkType& chunk) {
  utils::ReadLockGuard lk(lock_);

  auto it = attr_.chunks().find(index);
  if (it == attr_.chunks().end()) {
    return false;
  }

  chunk = it->second;

  return true;
}

bool Inode::UpdateIf(const AttrType& attr) {
  utils::WriteLockGuard lk(lock_);

  DINGO_LOG(INFO) << fmt::format("[inode.{}] update attr,this({}) version({}->{}).", attr_.ino(), (void*)this,
                                 attr_.version(), attr.version());
  if (attr.version() <= attr_.version()) {
    return false;
  }

  attr_ = attr;

  return true;
}

bool Inode::UpdateIf(AttrType&& attr) {
  utils::WriteLockGuard lk(lock_);

  DINGO_LOG(INFO) << fmt::format("[inode.{}] update attr,this({}) version({}->{}).", attr_.ino(), (void*)this,
                                 attr_.version(), attr.version());
  if (attr.version() <= attr_.version()) {
    return false;
  }

  attr_ = std::move(attr);

  return true;
}

Inode::AttrType Inode::Copy(bool just_basic) {
  utils::ReadLockGuard lk(lock_);

  if (!just_basic) {
    return attr_;
  }

  // Copy only basic attributes
  AttrType attr;
  attr.set_fs_id(attr_.fs_id());
  attr.set_ino(attr_.ino());
  attr.set_type(attr_.type());
  attr.set_length(attr_.length());
  attr.set_uid(attr_.uid());
  attr.set_gid(attr_.gid());
  attr.set_mode(attr_.mode());
  attr.set_nlink(attr_.nlink());
  attr.set_symlink(attr_.symlink());
  attr.set_rdev(attr_.rdev());
  attr.set_dtime(attr_.dtime());
  attr.set_ctime(attr_.ctime());
  attr.set_mtime(attr_.mtime());
  attr.set_atime(attr_.atime());
  attr.set_openmpcount(attr_.openmpcount());
  attr.mutable_parents()->CopyFrom(attr_.parents());
  attr.set_version(attr_.version());

  return std::move(attr);
}

Inode::AttrType&& Inode::Move() {
  utils::WriteLockGuard lk(lock_);

  return std::move(attr_);
}

InodeCache::InodeCache()
    : cache_(FLAGS_inode_cache_max_count, std::make_shared<utils::CacheMetrics>(kInodeCacheMetricsPrefix)) {}

InodeCache::~InodeCache() {}  // NOLINT

void InodeCache::PutInode(Ino ino, InodeSPtr inode) { cache_.Put(ino, inode); }

void InodeCache::DeleteInode(Ino ino) { cache_.Remove(ino); };

InodeSPtr InodeCache::GetInode(Ino ino) {
  InodeSPtr inode;
  if (!cache_.Get(ino, &inode)) {
    DINGO_LOG(INFO) << fmt::format("inode({}) not found.", ino);
    return nullptr;
  }

  return inode;
}

std::vector<InodeSPtr> InodeCache::GetInodes(std::vector<uint64_t> inoes) {
  std::vector<InodeSPtr> inodes;
  for (auto& ino : inoes) {
    InodeSPtr inode;
    if (cache_.Get(ino, &inode)) {
      inodes.push_back(inode);
    }
  }

  return inodes;
}

std::map<uint64_t, InodeSPtr> InodeCache::GetAllInodes() { return cache_.GetAll(); }

}  // namespace mdsv2
}  // namespace dingofs