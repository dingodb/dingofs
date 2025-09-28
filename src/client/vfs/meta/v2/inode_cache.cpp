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

#include "client/vfs/meta/v2/inode_cache.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "fmt/format.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "mds/common/helper.h"
#include "mds/common/logging.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

uint32_t Inode::FsId() {
  utils::ReadLockGuard lk(lock_);

  return attr_.fs_id();
}

uint64_t Inode::Ino() {
  utils::ReadLockGuard lk(lock_);

  return attr_.ino();
}

Inode::FileType Inode::Type() {
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

bool Inode::UpdateIf(const AttrEntry& attr) {
  utils::WriteLockGuard lk(lock_);

  DINGO_LOG(INFO) << fmt::format(
      "[meta.icache.{}] update attr,this({}) version({}->{}).", attr_.ino(),
      (void*)this, attr_.version(), attr.version());

  if (attr.version() <= attr_.version()) {
    DINGO_LOG(WARNING) << fmt::format(
        "[meta.icache.{}] version abnormal, old({}) new({}).", attr_.ino(),
        attr_.version(), attr.version());
    return false;
  }

  attr_ = attr;

  return true;
}

bool Inode::UpdateIf(AttrEntry&& attr) {
  utils::WriteLockGuard lk(lock_);

  DINGO_LOG(INFO) << fmt::format(
      "[meta.icache.{}] update attr, version({}->{}).", attr_.ino(),
      attr_.version(), attr.version());

  if (attr.version() <= attr_.version()) {
    DINGO_LOG(WARNING) << fmt::format(
        "[meta.icache.{}] version abnormal, old({}) new({}).", attr_.ino(),
        attr_.version(), attr.version());
    return false;
  }

  attr_ = std::move(attr);

  return true;
}

void Inode::ExpandLength(uint64_t length) {
  utils::WriteLockGuard lk(lock_);

  if (length <= attr_.length()) return;

  uint64_t now_ns = mds::Helper::TimestampNs();
  attr_.set_length(length);
  attr_.set_mtime(now_ns);
  attr_.set_ctime(now_ns);
  attr_.set_atime(now_ns);
}

Inode::AttrEntry Inode::Copy() {
  utils::ReadLockGuard lk(lock_);

  return attr_;
}

Inode::AttrEntry&& Inode::Move() {
  utils::WriteLockGuard lk(lock_);

  return std::move(attr_);
}

InodeCache::InodeCache(uint32_t fs_id) : fs_id_(fs_id) {}

InodeCache::~InodeCache() {}  // NOLINT

void InodeCache::UpsertInode(Ino ino, const AttrEntry& attr_entry) {
  LOG(INFO) << fmt::format("[meta.icache.{}.{}] upsert inode.", ino,
                           attr_entry.version());
  utils::WriteLockGuard lk(lock_);

  auto it = cache_.find(ino);
  if (it == cache_.end()) {
    cache_.emplace(ino, Inode::New(attr_entry));
  } else {
    it->second->UpdateIf(attr_entry);
  }
}

void InodeCache::DeleteInode(Ino ino) {
  LOG(INFO) << fmt::format("[meta.icache.{}] delete inode.", ino);

  utils::WriteLockGuard lk(lock_);

  cache_.erase(ino);
}

void InodeCache::Clear() {
  utils::WriteLockGuard lk(lock_);

  cache_.clear();
}

InodeSPtr InodeCache::GetInode(Ino ino) {
  utils::ReadLockGuard lk(lock_);

  auto it = cache_.find(ino);
  return (it == cache_.end()) ? nullptr : it->second;
}

std::vector<InodeSPtr> InodeCache::GetInodes(std::vector<uint64_t> inoes) {
  utils::ReadLockGuard lk(lock_);

  std::vector<InodeSPtr> inodes;
  inodes.reserve(inoes.size());
  for (auto& ino : inoes) {
    auto it = cache_.find(ino);
    if (it != cache_.end()) {
      inodes.push_back(it->second);
    }
  }

  return inodes;
}

bool InodeCache::Dump(Json::Value& value) {
  std::vector<InodeSPtr> inodes;
  {
    utils::ReadLockGuard lk(lock_);
    inodes.reserve(cache_.size());
    for (const auto& pair : cache_) {
      inodes.push_back(pair.second);
    }
  }
  Json::Value inode_array = Json::arrayValue;
  for (const auto& inode : inodes) {
    Json::Value item;
    item["fs_id"] = inode->FsId();
    item["ino"] = inode->Ino();
    item["type"] = pb::mds::FileType_Name(inode->Type());
    item["length"] = inode->Length();
    item["uid"] = inode->Uid();
    item["gid"] = inode->Gid();
    item["mode"] = inode->Mode();
    item["nlink"] = inode->Nlink();
    item["symlink"] = inode->Symlink();
    item["rdev"] = inode->Rdev();
    item["ctime"] = inode->Ctime();
    item["mtime"] = inode->Mtime();
    item["atime"] = inode->Atime();
    item["open_mp_count"] = inode->Openmpcount();
    item["version"] = inode->Version();
    inode_array.append(item);
  }
  value["inodes"] = inode_array;

  return true;
}

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs