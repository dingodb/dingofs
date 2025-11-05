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

static const std::string kInodeCacheMetricsPrefix = "dingofs_{}_inode_cache_";

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

bool Inode::UpdateIf(const AttrEntry& attr) {
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

bool Inode::UpdateIf(AttrEntry&& attr) {
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

InodeCache::InodeCache(uint32_t fs_id)
    : fs_id_(fs_id),
      cache_(FLAGS_mds_inode_cache_max_count,
             std::make_shared<utils::CacheMetrics>(fmt::format(kInodeCacheMetricsPrefix, fs_id))) {}

InodeCache::~InodeCache() {}  // NOLINT

void InodeCache::PutIf(Ino ino, InodeSPtr inode) {
  CHECK(inode != nullptr) << "old_inode is nullptr 001.";

  cache_.PutInplaceIf(ino, inode, [&](InodeSPtr& old_inode) {
    if (old_inode->Version() < inode->Version()) {
      old_inode->UpdateIf(inode->Copy());
    }
  });
}

void InodeCache::PutIf(AttrEntry& attr) { PutIf(attr.ino(), Inode::New(attr)); }

void InodeCache::Delete(Ino ino) { cache_.Remove(ino); };

void InodeCache::BatchDeleteIf(const std::function<bool(const Ino&)>& f) {
  DINGO_LOG(INFO) << fmt::format("[cache.inode.{}] batch delete inode.", fs_id_);

  cache_.BatchRemoveIf(f);
}

void InodeCache::Clear() {
  DINGO_LOG(INFO) << fmt::format("[cache.inode.{}] clear.", fs_id_);

  cache_.Clear();
}

InodeSPtr InodeCache::GetInode(Ino ino) {
  InodeSPtr inode;
  if (!cache_.Get(ino, &inode)) {
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

void InodeCache::DescribeByJson(Json::Value& value) {
  const auto metrics = cache_.GetCacheMetrics();

  value["cache_count"] = metrics->cacheCount.get_value();
  value["cache_bytes"] = metrics->cacheBytes.get_value();
  value["cache_hit"] = metrics->cacheHit.get_value();
  value["cache_miss"] = metrics->cacheMiss.get_value();
}

}  // namespace mds
}  // namespace dingofs