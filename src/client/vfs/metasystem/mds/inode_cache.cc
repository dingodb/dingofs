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

#include "client/vfs/metasystem/mds/inode_cache.h"

#include <atomic>
#include <cstdint>
#include <string>
#include <utility>

#include "client/vfs/metasystem/mds/helper.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "utils/concurrent/concurrent.h"
#include "utils/time.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

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

uint32_t Inode::Flags() {
  utils::ReadLockGuard lk(lock_);

  return attr_.flags();
}

uint64_t Inode::Version() {
  utils::ReadLockGuard lk(lock_);

  return attr_.version();
}

std::vector<uint64_t> Inode::Parents() {
  utils::ReadLockGuard lk(lock_);

  std::vector<uint64_t> parents;
  for (const auto& parent : attr_.parents()) {
    parents.push_back(parent);
  }

  return parents;
}

Inode::XAttrSet Inode::ListXAttrs() {
  utils::ReadLockGuard lk(lock_);

  Inode::XAttrSet xattrs;
  for (const auto& xattr : attr_.xattrs()) {
    xattrs.push_back(std::make_pair(xattr.first, xattr.second));
  }

  return xattrs;
}

std::string Inode::GetXAttr(const std::string& name) {
  utils::ReadLockGuard lk(lock_);

  auto it = attr_.xattrs().find(name);
  return (it != attr_.xattrs().end()) ? it->second : "";
}

void Inode::SetXAttr(const std::string& name, const std::string& value) {
  utils::WriteLockGuard lk(lock_);

  (*attr_.mutable_xattrs())[name] = value;
}

void Inode::RemoveXAttr(const std::string& name) {
  utils::WriteLockGuard lk(lock_);

  attr_.mutable_xattrs()->erase(name);
}

bool Inode::PutIf(const AttrEntry& attr) {
  utils::WriteLockGuard lk(lock_);

  LOG(INFO) << fmt::format(
      "[meta.icache.{}] update attr,this({}) version({}->{}).", attr_.ino(),
      (void*)this, attr_.version(), attr.version());

  if (attr.version() <= attr_.version()) {
    return false;
  }

  attr_ = attr;

  return true;
}

bool Inode::PutIf(AttrEntry&& attr) {
  utils::WriteLockGuard lk(lock_);

  LOG(INFO) << fmt::format(
      "[meta.icache.{}] update attr,this({}) version({}->{}).", attr_.ino(),
      (void*)this, attr_.version(), attr.version());

  if (attr.version() <= attr_.version()) {
    return false;
  }

  attr_ = std::move(attr);

  return true;
}

Attr Inode::ToAttr() {
  utils::ReadLockGuard lk(lock_);

  return Helper::ToAttr(attr_);
}

Inode::AttrEntry Inode::ToAttrEntry() {
  utils::ReadLockGuard lk(lock_);

  return attr_;
}

void Inode::UpdateLastAccessTime() {
  last_access_time_s_.store(utils::Timestamp(), std::memory_order_relaxed);
}

uint64_t Inode::LastAccessTimeS() {
  return last_access_time_s_.load(std::memory_order_relaxed);
}

void InodeCache::Put(Ino ino, const AttrEntry& attr) {
  shard_map_.withWLock(
      [ino, &attr](Map& map) mutable {
        auto it = map.find(ino);
        if (it == map.end()) {
          map.emplace(ino, Inode::New(attr));
        } else {
          it->second->PutIf(attr);
        }
      },
      ino);
}

void InodeCache::Delete(Ino ino) {
  LOG(INFO) << fmt::format("[meta.icache.{}] delete inode.", ino);

  shard_map_.withWLock([ino](Map& map) { map.erase(ino); }, ino);
}

InodeSPtr InodeCache::Get(Ino ino) {
  InodeSPtr inode;
  shard_map_.withRLock(
      [ino, &inode](Map& map) {
        auto it = map.find(ino);
        if (it != map.end()) {
          inode = it->second;
        }
      },
      ino);

  if (inode != nullptr) inode->UpdateLastAccessTime();

  return inode;
}

std::vector<InodeSPtr> InodeCache::Get(const std::vector<uint64_t>& inoes) {
  std::vector<InodeSPtr> inodes;
  for (const auto& ino : inoes) {
    shard_map_.withRLock(
        [ino, &inodes](Map& map) {
          auto it = map.find(ino);
          if (it != map.end()) {
            inodes.push_back(it->second);
          }
        },
        ino);
  }

  for (auto& inode : inodes) inode->UpdateLastAccessTime();

  return inodes;
}

void InodeCache::CleanExpired(uint64_t expire_s) {
  uint64_t now_s = utils::Timestamp();

  std::vector<InodeSPtr> inodes;
  shard_map_.iterate([&](const Map& map) {
    for (const auto& [_, inode] : map) {
      if (inode->LastAccessTimeS() + expire_s < now_s) {
        inodes.push_back(inode);
      }
    }
  });

  for (const auto& inode : inodes) {
    Delete(inode->Ino());
  }
}

bool InodeCache::Dump(Json::Value& value) {
  std::vector<InodeSPtr> inodes;

  shard_map_.iterate([&inodes](const Map& map) {
    for (const auto& [_, inode] : map) {
      inodes.push_back(inode);
    }
  });

  Json::Value inode_array = Json::arrayValue;
  for (const auto& inode : inodes) {
    Json::Value item;
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
    item["version"] = inode->Version();

    // parents
    Json::Value parent_array = Json::arrayValue;
    for (const auto& parent : inode->Parents()) {
      parent_array.append(parent);
    }
    item["parents"] = parent_array;

    // xattrs
    Json::Value xattr_array = Json::arrayValue;
    for (const auto& pair : inode->ListXAttrs()) {
      Json::Value xattr_item;
      xattr_item["name"] = pair.first;
      xattr_item["value"] = pair.second;
      xattr_array.append(xattr_item);
    }
    item["xattrs"] = xattr_array;

    inode_array.append(item);
  }
  value["inodes"] = inode_array;

  return true;
}

bool InodeCache::Load(const Json::Value& value) {
  for (const auto& item : value["inodes"]) {
    AttrEntry attr;
    attr.set_ino(item["ino"].asUInt64());
    Inode::FileType type;
    pb::mds::FileType_Parse(item["type"].asString(), &type);
    attr.set_type(type);
    attr.set_length(item["length"].asUInt64());
    attr.set_uid(item["uid"].asUInt());
    attr.set_gid(item["gid"].asUInt());
    attr.set_mode(item["mode"].asUInt());
    attr.set_nlink(item["nlink"].asUInt());
    attr.set_symlink(item["symlink"].asString());
    attr.set_rdev(item["rdev"].asUInt64());
    attr.set_ctime(item["ctime"].asUInt64());
    attr.set_mtime(item["mtime"].asUInt64());
    attr.set_atime(item["atime"].asUInt64());
    attr.set_version(item["version"].asUInt64());

    // parents
    for (const auto& parent : item["parents"]) {
      attr.add_parents(parent.asUInt64());
    }

    // xattrs
    for (const auto& xattr_item : item["xattrs"]) {
      (*attr.mutable_xattrs())[xattr_item["name"].asString()] =
          xattr_item["value"].asString();
    }

    Put(attr.ino(), attr);
  }

  return true;
}

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs