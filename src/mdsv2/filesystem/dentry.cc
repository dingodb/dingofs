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

#include "mdsv2/filesystem/dentry.h"

#include <glog/logging.h>

#include <cstdint>

#include "bthread/mutex.h"

namespace dingofs {
namespace mdsv2 {

Dentry::Dentry(uint32_t fs_id, const std::string& name, uint64_t parent_ino, uint64_t ino, pb::mdsv2::FileType type,
               uint32_t flag, InodePtr inode)
    : fs_id_(fs_id), name_(name), parent_ino_(parent_ino), ino_(ino), type_(type), flag_(flag), inode_(inode) {}

Dentry::Dentry(const pb::mdsv2::Dentry& dentry, InodePtr inode)
    : name_(dentry.name()),
      fs_id_(dentry.fs_id()),
      ino_(dentry.ino()),
      parent_ino_(dentry.parent_ino()),
      type_(dentry.type()),
      flag_(dentry.flag()),
      inode_(inode) {}

Dentry::Dentry(const Dentry& dentry, InodePtr inode)
    : name_(dentry.Name()),
      fs_id_(dentry.FsId()),
      ino_(dentry.Ino()),
      parent_ino_(dentry.ParentIno()),
      type_(dentry.Type()),
      flag_(dentry.Flag()),
      inode_(inode) {}

Dentry::~Dentry() {}  // NOLINT

pb::mdsv2::Dentry Dentry::CopyTo() {
  pb::mdsv2::Dentry dentry;

  dentry.set_fs_id(fs_id_);
  dentry.set_ino(ino_);
  dentry.set_parent_ino(parent_ino_);
  dentry.set_name(name_);
  dentry.set_type(type_);
  dentry.set_flag(flag_);

  return std::move(dentry);
}

InodePtr DentrySet::ParentInode() {
  CHECK(parent_inode_ != nullptr) << "parent inode is null.";

  return parent_inode_;
}

void DentrySet::PutChild(const Dentry& dentry) {
  utils::WriteLockGuard lk(lock_);

  auto it = children_.find(dentry.Name());
  if (it != children_.end()) {
    it->second = dentry;
  } else {
    children_[dentry.Name()] = dentry;
  }
}

void DentrySet::DeleteChild(const std::string& name) {
  utils::WriteLockGuard lk(lock_);

  children_.erase(name);
}

bool DentrySet::HasChild() {
  utils::ReadLockGuard lk(lock_);

  return !children_.empty();
}

bool DentrySet::GetChild(const std::string& name, Dentry& dentry) {
  utils::ReadLockGuard lk(lock_);

  auto it = children_.find(name);
  if (it == children_.end()) {
    return false;
  }

  dentry = it->second;
  return true;
}

std::vector<Dentry> DentrySet::GetChildren(const std::string& start_name, uint32_t limit, bool is_only_dir) {
  utils::ReadLockGuard lk(lock_);

  std::vector<Dentry> dentries;
  dentries.reserve(limit);

  for (auto it = children_.upper_bound(start_name); it != children_.end() && dentries.size() < limit; ++it) {
    if (is_only_dir && it->second.Type() != pb::mdsv2::FileType::DIRECTORY) {
      continue;
    }

    dentries.push_back(it->second);
  }

  return std::move(dentries);
}

std::vector<Dentry> DentrySet::GetAllChildren() {
  utils::ReadLockGuard lk(lock_);

  std::vector<Dentry> dentries;
  dentries.reserve(children_.size());

  for (const auto& [name, dentry] : children_) {
    dentries.push_back(dentry);
  }

  return dentries;
}

DentryCache::DentryCache() : cache_(0) {}
DentryCache::~DentryCache() {}  // NOLINT

void DentryCache::Put(uint64_t ino, DentrySetPtr dentry_set) { cache_.Put(ino, dentry_set); }

void DentryCache::Delete(uint64_t ino) { cache_.Remove(ino); }

DentrySetPtr DentryCache::Get(uint64_t ino) {
  DentrySetPtr dentry_set;
  if (!cache_.Get(ino, &dentry_set)) {
    return nullptr;
  }

  return dentry_set;
}

}  // namespace mdsv2
}  // namespace dingofs