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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_V2_PARENT_CACHE_H_
#define DINGOFS_SRC_CLIENT_VFS_META_V2_PARENT_CACHE_H_

#include <sys/types.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "client/vfs/vfs_meta.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

class ParentCache;
using ParentCachePtr = std::shared_ptr<ParentCache>;

class ParentCache {
 public:
  ParentCache();
  ~ParentCache() = default;

  struct Entry {
    int64_t parent;
    uint64_t version;
  };

  static ParentCachePtr New() { return std::make_shared<ParentCache>(); }

  bool GetParent(Ino ino, int64_t& parent);
  bool GetVersion(Ino ino, uint64_t& version);
  std::vector<uint64_t> GetAncestors(uint64_t ino);

  void Upsert(Ino ino, int64_t parent);
  void Upsert(Ino ino, uint64_t version);
  void Upsert(Ino ino, int64_t parent, uint64_t version);
  void Delete(Ino ino);

 private:
  utils::RWLock lock_;
  // ino -> entry
  std::unordered_map<int64_t, Entry> ino_map_;
};

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_V2_PARENT_CACHE_H_