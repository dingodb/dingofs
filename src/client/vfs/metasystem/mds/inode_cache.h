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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_V2_INODE_CACHE_H_
#define DINGOFS_SRC_CLIENT_VFS_META_V2_INODE_CACHE_H_

#include <sys/types.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "client/vfs/vfs_meta.h"
#include "json/value.h"
#include "mds/common/type.h"
#include "utils/concurrent/concurrent.h"
#include "utils/shards.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

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

  Inode(const AttrEntry& attr) { attr_ = attr; }
  Inode(AttrEntry&& attr) { attr_ = std::move(attr); }
  ~Inode() = default;

  static InodeSPtr New(const AttrEntry& inode) {
    return std::make_shared<Inode>(inode);
  }

  uint64_t Ino();
  FileType Type();
  uint64_t Length();
  uint32_t Uid();
  uint32_t Gid();
  uint32_t Mode();
  uint32_t Nlink();
  std::string Symlink();

  uint64_t Rdev();
  uint64_t Ctime();
  uint64_t Mtime();
  uint64_t Atime();
  uint32_t Flags();
  uint64_t Version();

  std::vector<uint64_t> Parents();

  XAttrSet ListXAttrs();
  std::string GetXAttr(const std::string& name);
  void SetXAttr(const std::string& name, const std::string& value);
  void RemoveXAttr(const std::string& name);

  bool PutIf(const AttrEntry& attr);
  bool PutIf(AttrEntry&& attr);

  Attr ToAttr();
  AttrEntry ToAttrEntry();

  void UpdateLastAccessTime();
  uint64_t LastAccessTimeS();

 private:
  utils::RWLock lock_;

  AttrEntry attr_;

  std::atomic<uint64_t> last_access_time_s_{0};
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

  void Put(Ino ino, const AttrEntry& attr);
  void Delete(Ino ino);

  InodeSPtr Get(Ino ino);
  std::vector<InodeSPtr> Get(const std::vector<uint64_t>& inoes);

  void CleanExpired(uint64_t expire_s);

  bool Dump(Json::Value& value);
  bool Load(const Json::Value& value);

 private:
  using Map = absl::btree_map<Ino, InodeSPtr>;

  const uint32_t fs_id_{0};

  constexpr static size_t kShardNum = 32;
  utils::Shards<Map, kShardNum> shard_map_;
};

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_V2_INODE_CACHE_H_