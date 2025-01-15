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

Dentry::Dentry(uint32_t fs_id, const std::string& name, uint64_t parent_ino, uint64_t ino, pb::mdsv2::FileType type)
    : fs_id_(fs_id), name_(name), parent_ino_(parent_ino), ino_(ino), type_(type) {
  CHECK(bthread_mutex_init(&mutex_, nullptr) == 0) << "init mutex fail.";
}

Dentry::Dentry(const pb::mdsv2::Dentry& dentry)
    : name_(dentry.name()),
      fs_id_(dentry.fs_id()),
      ino_(dentry.inode_id()),
      parent_ino_(dentry.parent_inode_id()),
      type_(dentry.type()),
      flag_(dentry.flag()) {
  CHECK(bthread_mutex_init(&mutex_, nullptr) == 0) << "init mutex fail.";
}

Dentry::~Dentry() { CHECK(bthread_mutex_destroy(&mutex_) == 0) << "destroy mutex fail."; }

bool Dentry::AddChildDentry(DentryPtr dentry, bool is_force) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = child_dentries_.find(dentry->GetName());
  if (it != child_dentries_.end()) {
    if (!is_force) {
      return false;
    }

    it->second = dentry;
  } else {
    child_dentries_[dentry->GetName()] = dentry;
  }

  return true;
}

bool Dentry::DeleteChildDentry(const std::string& name) {
  BAIDU_SCOPED_LOCK(mutex_);

  child_dentries_.erase(name);

  return true;
}

DentryPtr Dentry::GetChildDentry(const std::string& name) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = child_dentries_.find(name);
  return it != child_dentries_.end() ? it->second : nullptr;
}

std::vector<DentryPtr> Dentry::GetChildDentries(const std::string& start_name, uint32_t limit, bool is_only_dir) {
  BAIDU_SCOPED_LOCK(mutex_);

  std::vector<DentryPtr> dentries;
  dentries.reserve(limit);
  for (auto it = child_dentries_.find(start_name); it != child_dentries_.end() && dentries.size() < limit; ++it) {
    if (is_only_dir && it->second->GetType() != pb::mdsv2::FileType::DIRECTORY) {
      continue;
    }

    dentries.push_back(it->second);
  }

  return dentries;
}

pb::mdsv2::Dentry Dentry::GenPBDentry() {
  pb::mdsv2::Dentry dentry;

  dentry.set_fs_id(fs_id_);
  dentry.set_inode_id(ino_);
  dentry.set_parent_inode_id(parent_ino_);
  dentry.set_name(name_);
  dentry.set_type(type_);
  dentry.set_flag(flag_);

  return dentry;
}

DentryMap::DentryMap() { CHECK(bthread_mutex_init(&mutex_, nullptr) == 0) << "init mutex fail."; }

DentryMap::~DentryMap() { CHECK(bthread_mutex_destroy(&mutex_) == 0) << "destory mutex fail."; }

DentryPtr DentryMap::GetDentry(uint64_t ino) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto iter = dentry_map_.find(ino);
  if (iter == dentry_map_.end()) {
    return nullptr;
  }

  return iter->second;
}

DentryPtr DentryMap::GetDentry(const std::string& name) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto name_it = name_ino_map_.find(name);
  if (name_it == name_ino_map_.end()) {
    return nullptr;
  }

  uint64_t ino = name_it->second;

  auto it = dentry_map_.find(ino);
  if (it == dentry_map_.end()) {
    return nullptr;
  }

  return it->second;
}

void DentryMap::AddDentry(DentryPtr dentry) {
  BAIDU_SCOPED_LOCK(mutex_);

  dentry_map_[dentry->GetIno()] = dentry;
  name_ino_map_[dentry->GetName()] = dentry->GetIno();
}

void DentryMap::DeleteDentry(const std::string& name) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto name_it = name_ino_map_.find(name);
  if (name_it == name_ino_map_.end()) {
    return;
  }

  uint64_t ino = name_it->second;
  dentry_map_.erase(ino);
  name_ino_map_.erase(name);
}

}  // namespace mdsv2
}  // namespace dingofs