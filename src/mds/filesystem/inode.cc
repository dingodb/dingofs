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

#include "mds/filesystem/inode.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "brpc/reloadable_flags.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "mds/common/helper.h"
#include "mds/common/logging.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace mds {

static const std::string kInodeCacheMetricsPrefix = "dingofs_{}_inode_cache_{}";

// 0: no limit
DEFINE_uint32(mds_inode_cache_max_count, 4 * 1024 * 1024, "inode cache max count");
DEFINE_validator(mds_inode_cache_max_count, brpc::PassValidate);

uint32_t Inode::FsId() {
  utils::ReadLockGuard lk(lock_);

  return attr_.fs_id();
}

uint64_t Inode::Ino() {
  utils::ReadLockGuard lk(lock_);

  return attr_.ino();
}

FileType Inode::Type() {
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

uint32_t Inode::Flags() {
  utils::ReadLockGuard lk(lock_);

  return attr_.flags();
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

bool Inode::PutIf(const AttrEntry& attr) {
  utils::WriteLockGuard lk(lock_);

  DINGO_LOG(INFO) << fmt::format("[inode.{}] update attr,this({}) version({}->{}).", attr_.ino(), (void*)this,
                                 attr_.version(), attr.version());

  if (attr.version() <= attr_.version()) {
    DINGO_LOG(DEBUG) << fmt::format("[inode.{}] version abnormal, old({}) new({}).", attr_.ino(), attr_.version(),
                                    attr.version());
    return false;
  }

  attr_ = attr;

  return true;
}

bool Inode::PutIf(AttrEntry&& attr) {
  utils::WriteLockGuard lk(lock_);

  DINGO_LOG(INFO) << fmt::format("[inode.{}] update attr,this({}) version({}->{}).", attr_.ino(), (void*)this,
                                 attr_.version(), attr.version());

  if (attr.version() <= attr_.version()) {
    DINGO_LOG(DEBUG) << fmt::format("[inode.{}] version abnormal, old({}) new({}).", attr_.ino(), attr_.version(),
                                    attr.version());
    return false;
  }

  attr_ = std::move(attr);

  return true;
}

void Inode::ExpandLength(uint64_t length) {
  utils::WriteLockGuard lk(lock_);

  if (length <= attr_.length()) return;

  uint64_t now_ns = Helper::TimestampNs();
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

void Inode::UpdateLastAccessTime() { last_access_time_s_.store(Helper::Timestamp(), std::memory_order_relaxed); }

uint64_t Inode::LastAccessTimeS() { return last_access_time_s_.load(std::memory_order_relaxed); }

InodeCache::InodeCache(uint32_t fs_id)
    : fs_id_(fs_id),
      access_miss_count_(fmt::format(kInodeCacheMetricsPrefix, fs_id, "miss_count")),
      access_hit_count_(fmt::format(kInodeCacheMetricsPrefix, fs_id, "hit_count")),
      clean_count_(fmt::format(kInodeCacheMetricsPrefix, fs_id, "clean_count")) {}

InodeCache::~InodeCache() {}  // NOLINT

void InodeCache::PutIf(Ino ino, InodeSPtr& inode) {
  CHECK(inode != nullptr) << "inode is nullptr.";

  shard_map_.withWLock(
      [ino, &inode](Map& map) mutable {
        auto it = map.find(ino);
        if (it == map.end()) {
          map.emplace(ino, inode);
        } else {
          it->second->PutIf(inode->Copy());
        }
      },
      ino);
}

void InodeCache::PutIf(AttrEntry&& attr) {  // NOLINT
  shard_map_.withWLock(
      [&](Map& map) mutable {
        auto it = map.find(attr.ino());
        if (it == map.end()) {
          const Ino ino = attr.ino();
          map.emplace(ino, Inode::New(std::move(attr)));
        } else {
          it->second->PutIf(std::move(attr));
        }
      },
      attr.ino());
}

void InodeCache::PutIf(const AttrEntry& attr) {
  shard_map_.withWLock(
      [&](Map& map) mutable {
        auto it = map.find(attr.ino());
        if (it == map.end()) {
          map.emplace(attr.ino(), Inode::New(attr));
        } else {
          it->second->PutIf(attr);
        }
      },
      attr.ino());
}

void InodeCache::Delete(Ino ino) {
  shard_map_.withWLock([ino](Map& map) { map.erase(ino); }, ino);
};

void InodeCache::DeleteIf(std::function<bool(const Ino&)>&& f) {  // NOLINT
  DINGO_LOG(INFO) << fmt::format("[cache.inode.{}] batch delete inode.", fs_id_);

  shard_map_.iterateWLock([&](Map& map) {
    for (auto it = map.begin(); it != map.end();) {
      if (f(it->first)) {
        auto temp_it = it++;
        map.erase(temp_it);
      } else {
        ++it;
      }
    }
  });
}

void InodeCache::Clear() {
  DINGO_LOG(INFO) << fmt::format("[cache.inode.{}] clear.", fs_id_);

  shard_map_.iterateWLock([&](Map& map) { map.clear(); });
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

  if (inode != nullptr) {
    inode->UpdateLastAccessTime();
    access_hit_count_ << 1;

  } else {
    access_miss_count_ << 1;
  }

  return inode;
}

std::vector<InodeSPtr> InodeCache::Get(std::vector<uint64_t> inoes) {
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

  access_hit_count_ << inodes.size();
  access_miss_count_ << (inoes.size() - inodes.size());

  return inodes;
}

std::vector<InodeSPtr> InodeCache::GetAll() {
  std::vector<InodeSPtr> inodes;

  shard_map_.iterate([&inodes](const Map& map) {
    for (const auto& [_, inode] : map) {
      inodes.push_back(inode);
    }
  });

  return inodes;
}

size_t InodeCache::Size() {
  size_t size = 0;
  shard_map_.iterate([&size](const Map& map) { size += map.size(); });

  return size;
}

void InodeCache::CleanExpired(uint64_t expire_s) {
  uint64_t now_s = Helper::Timestamp();

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

  clean_count_ << inodes.size();
}

void InodeCache::DescribeByJson(Json::Value& value) {
  value["cache_count"] = Size();
  value["cache_hit"] = access_hit_count_.get_value();
  value["cache_miss"] = access_miss_count_.get_value();
  value["cache_clean"] = clean_count_.get_value();
}

}  // namespace mds
}  // namespace dingofs