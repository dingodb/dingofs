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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_MDS_INODE_CACHE_H_
#define DINGOFS_SRC_CLIENT_VFS_META_MDS_INODE_CACHE_H_

#include <sys/types.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/inlined_vector.h"
#include "client/vfs/vfs_meta.h"
#include "json/value.h"
#include "mds/common/type.h"
#include "utils/concurrent/concurrent.h"
#include "utils/shards.h"
#include "utils/time.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

class Inode;
using InodeSPtr = std::shared_ptr<Inode>;

class InodeCache;
using InodeCacheUPtr = std::unique_ptr<InodeCache>;

using mds::Ino;

class Inode {
 public:
  using FileType = pb::mds::FileType;
  using AttrEntry = mds::AttrEntry;
  using XAttrMap = ::google::protobuf::Map<std::string, std::string>;
  using XAttrSet = std::vector<std::pair<std::string, std::string>>;

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
        parents_(attr.parents().begin(), attr.parents().end()),
        last_active_time_s_(utils::Timestamp()) {
    for (const auto& xattr : attr.xattrs()) {
      xattrs_.emplace(xattr.first, xattr.second);
    }
  }

  ~Inode() = default;

  static InodeSPtr New(const AttrEntry& inode) {
    return std::make_shared<Inode>(inode);
  }

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

  bool IsDeleted() const {
    utils::ReadLockGuard lk(lock_);

    return nlink_ == 0;
  }

  std::vector<uint64_t> Parents() const {
    utils::ReadLockGuard lk(lock_);

    return std::vector<uint64_t>(parents_.begin(), parents_.end());
  }

  XAttrSet ListXAttrs() const {
    utils::ReadLockGuard lk(lock_);

    Inode::XAttrSet xattrs;
    for (const auto& [key, value] : xattrs_) {
      xattrs.push_back(std::make_pair(key, value));
    }

    return xattrs;
  }

  std::string GetXAttr(const std::string& name) const {
    utils::ReadLockGuard lk(lock_);

    auto it = xattrs_.find(name);
    return (it != xattrs_.end()) ? it->second : "";
  }

  void SetXAttr(const std::string& name, const std::string& value) {
    utils::WriteLockGuard lk(lock_);

    xattrs_[name] = value;
  }

  void RemoveXAttr(const std::string& name) {
    utils::WriteLockGuard lk(lock_);

    xattrs_.erase(name);
  }

  bool PutIf(const AttrEntry& attr);

  Attr ToAttr() const;
  AttrEntry ToAttrEntry() const;

  void UpdateLastAccessTime();
  uint64_t GetlastActiveTime();

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

class InodeCache {
 public:
  using AttrEntry = mds::AttrEntry;

  InodeCache(uint32_t fs_id) : fs_id_(fs_id) {}
  ~InodeCache() = default;

  InodeCache(const InodeCache&) = delete;
  InodeCache& operator=(const InodeCache&) = delete;
  InodeCache(InodeCache&&) = delete;
  InodeCache& operator=(InodeCache&&) = delete;

  static InodeCacheUPtr New(uint32_t fs_id) {
    return std::make_unique<InodeCache>(fs_id);
  }

  InodeSPtr Put(Ino ino, const AttrEntry& attr);
  void Delete(Ino ino);

  InodeSPtr Get(Ino ino);
  std::vector<InodeSPtr> Get(const std::vector<uint64_t>& inoes);

  void CleanExpired(uint64_t expire_s);

  size_t Size();
  size_t Bytes();

  void Summary(Json::Value& value);
  bool Dump(Json::Value& value);
  bool Load(const Json::Value& value);

 private:
  using Map = absl::btree_map<Ino, InodeSPtr>;

  const uint32_t fs_id_{0};

  constexpr static size_t kShardNum = 32;
  utils::Shards<Map, kShardNum> shard_map_;

  // metrics
  bvar::Adder<uint64_t> total_count_{"meta_inode_total_count"};
  bvar::Adder<uint64_t> clean_count_{"meta_inode_clean_count"};
};

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_MDS_INODE_CACHE_H_