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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_MDS_DENTRY_CACHE_H_
#define DINGOFS_SRC_CLIENT_VFS_META_MDS_DENTRY_CACHE_H_

#include "absl/container/btree_map.h"
#include "client/vfs/vfs_meta.h"
#include "json/value.h"
#include "utils/shards.h"
#include "utils/time.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

// accelerate lookup
class DentryCache {
 public:
  DentryCache() = default;
  ~DentryCache() = default;

  void Put(Ino parent_ino, const std::string& name, Ino ino);
  void Delete(Ino parent_ino, const std::string& name);

  Ino Get(Ino parent_ino, const std::string& name);
  std::vector<Ino> ListFile(Ino parent_ino);

  void CleanExpired(uint64_t expire_s);

  size_t Size();
  size_t Bytes();

  void Summary(Json::Value& value);

 private:
  struct Key {
    Ino parent_ino{0};
    std::string name;

    Key(Ino p, const std::string& n) : parent_ino(p), name(n) {}
  };

  struct KeyCompare {
    bool operator()(const Key& a, const Key& b) const {
      if (a.parent_ino != b.parent_ino) {
        return a.parent_ino < b.parent_ino;
      }
      return a.name < b.name;
    }
  };

  struct Value {
    Ino ino{0};
    uint64_t last_fresh_s{0};

    Value(Ino ino) : ino(ino), last_fresh_s(utils::Timestamp()) {}
  };

  using Map = absl::btree_map<Key, Value, KeyCompare>;

  constexpr static size_t kShardNum = 64;
  utils::Shards<Map, kShardNum> shard_map_;

  // metric
  bvar::Adder<uint64_t> total_count_{"meta_dentry_cache_total_count"};
  bvar::Adder<uint64_t> clean_count_{"meta_dentry_cache_clean_count"};
};

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_MDS_DENTRY_CACHE_H_
