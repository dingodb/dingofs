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
#include <string>
#include <utility>

#include "brpc/reloadable_flags.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "utils/concurrent/concurrent.h"
#include "utils/time.h"

namespace dingofs {
namespace mds {

static const std::string kInodeCacheMetricsPrefix = "dingofs_{}_inode_cache_{}";

// 0: no limit
DEFINE_uint32(mds_inode_cache_max_count, 4 * 1024 * 1024, "inode cache max count");
DEFINE_validator(mds_inode_cache_max_count, brpc::PassValidate);

bool Inode::PutIf(const AttrEntry& attr) {
  utils::WriteLockGuard lk(lock_);

  LOG(INFO) << fmt::format("[inode.{}] update attr,this({}) version({}->{}).", ino_, (void*)this, version_,
                           attr.version());

  if (attr.version() <= version_) {
    LOG_DEBUG << fmt::format("[inode.{}] version abnormal, old({}) new({}).", ino_, version_, attr.version());
    return false;
  }

  // clone new attr
  if (length_ != attr.length()) length_ = attr.length();
  if (ctime_ != attr.ctime()) ctime_ = attr.ctime();
  if (mtime_ != attr.mtime()) mtime_ = attr.mtime();
  if (atime_ != attr.atime()) atime_ = attr.atime();
  if (uid_ != attr.uid()) uid_ = attr.uid();
  if (gid_ != attr.gid()) gid_ = attr.gid();
  if (mode_ != attr.mode()) mode_ = attr.mode();
  if (nlink_ != attr.nlink()) nlink_ = attr.nlink();
  if (symlink_ != attr.symlink()) symlink_ = attr.symlink();
  if (rdev_ != attr.rdev()) rdev_ = attr.rdev();
  if (flags_ != attr.flags()) flags_ = attr.flags();
  if (maybe_tiny_file_ != attr.maybe_tiny_file()) maybe_tiny_file_ = attr.maybe_tiny_file();

  parents_.clear();
  parents_.insert(parents_.end(), attr.parents().begin(), attr.parents().end());

  xattrs_.clear();
  for (const auto& xattr : attr.xattrs()) {
    xattrs_.emplace(xattr.first, xattr.second);
  }

  version_ = attr.version();

  return true;
}

void Inode::ExpandLength(uint64_t length) {
  utils::WriteLockGuard lk(lock_);

  if (length <= length_) return;

  length_ = length;

  uint64_t now_ns = utils::TimestampNs();
  mtime_ = now_ns;
  ctime_ = now_ns;
  atime_ = now_ns;
}

Inode::AttrEntry Inode::Copy() {
  utils::ReadLockGuard lk(lock_);

  Inode::AttrEntry attr;
  attr.set_fs_id(fs_id_);
  attr.set_ino(ino_);
  attr.set_length(length_);
  attr.set_ctime(ctime_);
  attr.set_mtime(mtime_);
  attr.set_atime(atime_);
  attr.set_uid(uid_);
  attr.set_gid(gid_);
  attr.set_mode(mode_);
  attr.set_nlink(nlink_);
  attr.set_type(type_);
  attr.set_symlink(symlink_);
  attr.set_rdev(rdev_);
  attr.set_flags(flags_);
  attr.set_maybe_tiny_file(maybe_tiny_file_);
  for (const auto& parent : parents_) {
    attr.add_parents(parent);
  }
  for (const auto& [key, value] : xattrs_) {
    (*attr.mutable_xattrs())[key] = value;
  }
  attr.set_version(version_);

  return attr;
}

InodeCache::InodeCache(uint32_t fs_id)
    : fs_id_(fs_id),
      total_count_(fmt::format(kInodeCacheMetricsPrefix, fs_id, "total_count")),
      access_miss_count_(fmt::format(kInodeCacheMetricsPrefix, fs_id, "miss_count")),
      access_hit_count_(fmt::format(kInodeCacheMetricsPrefix, fs_id, "hit_count")),
      clean_count_(fmt::format(kInodeCacheMetricsPrefix, fs_id, "clean_count")) {}

InodeCache::~InodeCache() {}  // NOLINT

void InodeCache::PutIf(Ino ino, InodeSPtr& inode) {
  CHECK(inode != nullptr) << "inode is nullptr.";

  shard_map_.withWLock(
      [this, ino, &inode](Map& map) mutable {
        auto it = map.find(ino);
        if (it == map.end()) {
          map.emplace(ino, inode);
          total_count_ << 1;

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
          map.emplace(ino, Inode::New(attr));
          total_count_ << 1;

        } else {
          it->second->PutIf(attr);
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
          total_count_ << 1;

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
  LOG(INFO) << fmt::format("[cache.inode.{}] batch delete inode.", fs_id_);

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
  LOG(INFO) << fmt::format("[cache.inode.{}] clear.", fs_id_);

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
    inode->UpdateLastActiveTime();
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

  for (auto& inode : inodes) inode->UpdateLastActiveTime();

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

size_t InodeCache::Bytes() { return Size() * (sizeof(Inode) + sizeof(Ino)); }

void InodeCache::CleanExpired(uint64_t expire_s) {
  if (Size() < FLAGS_mds_inode_cache_max_count) return;

  std::vector<InodeSPtr> inodes;
  shard_map_.iterate([&](const Map& map) {
    for (const auto& [_, inode] : map) {
      if (inode->LastActiveTimeS() < expire_s) {
        inodes.push_back(inode);
      }
    }
  });

  for (const auto& inode : inodes) {
    Delete(inode->Ino());
  }

  clean_count_ << inodes.size();

  LOG(INFO) << fmt::format("[cache.inode.{}] clean expired, stat({}|{}|{}).", fs_id_, Size(), inodes.size(),
                           clean_count_.get_value());
}

void InodeCache::DescribeByJson(Json::Value& value) {
  value["cache_count"] = Size();
  value["cache_hit"] = access_hit_count_.get_value();
  value["cache_miss"] = access_miss_count_.get_value();
  value["cache_clean"] = clean_count_.get_value();
}

void InodeCache::Summary(Json::Value& value) {
  value["name"] = "inodecache";
  value["count"] = Size();
  value["bytes"] = Bytes();
  value["total_count"] = total_count_.get_value();
  value["clean_count"] = clean_count_.get_value();
  value["hit_count"] = access_hit_count_.get_value();
  value["miss_count"] = access_miss_count_.get_value();
}

}  // namespace mds
}  // namespace dingofs