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

#ifndef DINGOFS_MDS_FILESYSTEM_INODE_H_
#define DINGOFS_MDS_FILESYSTEM_INODE_H_

#include <sys/types.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "json/value.h"
#include "mds/common/type.h"
#include "utils/concurrent/concurrent.h"
#include "utils/shards.h"
#include "utils/time.h"

namespace dingofs {
namespace mds {

class Inode;
using InodeSPtr = std::shared_ptr<Inode>;
using InodeWPtr = std::weak_ptr<Inode>;

class Inode {
 public:
  using AttrEntry = mds::AttrEntry;
  using XAttrMap = ::google::protobuf::Map<std::string, std::string>;
  using ChunkMap = ::google::protobuf::Map<uint64_t, ChunkEntry>;

  Inode(const AttrEntry& attr)
      : fs_id_(attr.fs_id()),
        ino_(attr.ino()),
        type_(attr.type()),
        length_(attr.length()),
        ctime_(attr.ctime()),
        mtime_(attr.mtime()),
        atime_(attr.atime()),
        uid_(attr.uid()),
        gid_(attr.gid()),
        mode_(attr.mode()),
        nlink_(attr.nlink()),
        symlink_(attr.symlink()),
        rdev_(attr.rdev()),
        flags_(attr.flags()),
        maybe_tiny_file_(attr.maybe_tiny_file()),
        version_(attr.version()),
        parents_(attr.parents().begin(), attr.parents().end()) {
    last_active_time_s_ = utils::Timestamp();
    for (const auto& xattr : attr.xattrs()) {
      xattrs_.emplace(xattr.first, xattr.second);
    }
  }
  ~Inode() = default;

  static InodeSPtr New(const AttrEntry& inode) { return std::make_shared<Inode>(inode); }

  uint32_t FsId() const { return fs_id_; }
  uint64_t Ino() const { return ino_; }
  FileType Type() const { return type_; }

  uint64_t Length() const {
    utils::ReadLockGuard lk(lock_);
    return length_;
  }
  uint32_t Uid() const {
    utils::ReadLockGuard lk(lock_);
    return uid_;
  }
  uint32_t Gid() const {
    utils::ReadLockGuard lk(lock_);
    return gid_;
  }
  uint32_t Mode() const {
    utils::ReadLockGuard lk(lock_);
    return mode_;
  }
  uint32_t Nlink() const {
    utils::ReadLockGuard lk(lock_);
    return nlink_;
  }
  std::string Symlink() const {
    utils::ReadLockGuard lk(lock_);
    return symlink_;
  }
  uint64_t Rdev() const {
    utils::ReadLockGuard lk(lock_);
    return rdev_;
  }
  uint64_t Ctime() const {
    utils::ReadLockGuard lk(lock_);

    return ctime_;
  }
  uint64_t Mtime() const {
    utils::ReadLockGuard lk(lock_);
    return mtime_;
  }
  uint64_t Atime() const {
    utils::ReadLockGuard lk(lock_);
    return atime_;
  }
  uint32_t Flags() const {
    utils::ReadLockGuard lk(lock_);
    return flags_;
  }
  bool MaybeTinyFile() const {
    utils::ReadLockGuard lk(lock_);
    return maybe_tiny_file_;
  }
  uint64_t Version() const {
    utils::ReadLockGuard lk(lock_);
    return version_;
  }

  XAttrMap XAttrs() const {
    utils::ReadLockGuard lk(lock_);

    XAttrMap xattrs;
    for (const auto& [key, value] : xattrs_) {
      xattrs.emplace(key, value);
    }

    return xattrs;
  }

  std::string XAttr(const std::string& name) const {
    utils::ReadLockGuard lk(lock_);

    auto it = xattrs_.find(name);
    return (it != xattrs_.end()) ? it->second : "";
  }

  bool PutIf(const AttrEntry& attr);

  void ExpandLength(uint64_t length);

  AttrEntry Copy();

  void UpdateLastActiveTime() { last_active_time_s_.store(utils::Timestamp(), std::memory_order_relaxed); }
  uint64_t LastActiveTimeS() { return last_active_time_s_.load(std::memory_order_relaxed); }

 private:
  mutable utils::RWLock lock_;

  const uint32_t fs_id_{0};
  const mds::Ino ino_{0};
  const FileType type_;

  uint32_t uid_{0};
  uint32_t gid_{0};
  uint32_t mode_{0};
  uint32_t nlink_{0};
  std::string symlink_;
  uint64_t rdev_{0};
  uint32_t flags_;
  bool maybe_tiny_file_{false};

  static constexpr size_t kDefaultParentNum = 8;
  absl::InlinedVector<mds::Ino, kDefaultParentNum> parents_;
  absl::flat_hash_map<std::string, std::string> xattrs_;

  uint64_t length_{0};
  uint64_t ctime_{0};
  uint64_t mtime_{0};
  uint64_t atime_{0};

  uint64_t version_{0};

  std::atomic<uint64_t> last_active_time_s_{0};
};

class InodeCache;
using InodeCacheSPtr = std::shared_ptr<InodeCache>;

// cache all file/dir inode
class InodeCache {
 public:
  InodeCache(uint32_t fs_id);
  ~InodeCache();

  InodeCache(const InodeCache&) = delete;
  InodeCache& operator=(const InodeCache&) = delete;
  InodeCache(InodeCache&&) = delete;
  InodeCache& operator=(InodeCache&&) = delete;

  static InodeCacheSPtr New(uint32_t fs_id) { return std::make_shared<InodeCache>(fs_id); }

  void PutIf(Ino ino, InodeSPtr& inode);
  void PutIf(AttrEntry&& attr);
  void PutIf(const AttrEntry& attr);

  void Delete(Ino ino);
  void DeleteIf(std::function<bool(const Ino&)>&& f);

  void Clear();

  InodeSPtr Get(Ino ino);
  std::vector<InodeSPtr> Get(std::vector<uint64_t> inoes);
  std::vector<InodeSPtr> GetAll();

  size_t Size();
  size_t Bytes();

  void CleanExpired(uint64_t expire_s);

  void DescribeByJson(Json::Value& value);
  void Summary(Json::Value& value);

 private:
  using Map = absl::flat_hash_map<Ino, InodeSPtr>;

  const uint32_t fs_id_{0};

  constexpr static size_t kShardNum = 64;
  utils::Shards<Map, kShardNum> shard_map_;

  // metric
  bvar::Adder<int64_t> total_count_;
  bvar::Adder<int64_t> clean_count_;
  bvar::Adder<int64_t> access_miss_count_;
  bvar::Adder<int64_t> access_hit_count_;
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_FILESYSTEM_INODE_H_