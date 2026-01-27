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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_MDS_PARENT_MEMO_H_
#define DINGOFS_SRC_CLIENT_VFS_META_MDS_PARENT_MEMO_H_

#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "client/vfs/vfs_meta.h"
#include "json/value.h"
#include "utils/shards.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

class ParentMemo {
 public:
  ParentMemo();
  ~ParentMemo() = default;

  bool GetParent(Ino ino, Ino& parent);
  bool GetVersion(Ino ino, uint64_t& version);
  std::vector<uint64_t> GetAncestors(uint64_t ino);
  bool GetRenameRefCount(Ino ino, int32_t& rename_ref_count);

  void Upsert(Ino ino, Ino parent);
  void UpsertVersion(Ino ino, uint64_t version);
  void UpsertVersionAndRenameRefCount(Ino ino, uint64_t version);
  void Upsert(Ino ino, Ino parent, uint64_t version);
  void Delete(Ino ino);
  void DecRenameRefCount(Ino ino);

  size_t Size();
  size_t Bytes();

  void Summary(Json::Value& value);
  bool Dump(Json::Value& value);
  bool Load(const Json::Value& value);

 private:
  struct Entry {
    Ino parent;
    uint64_t version;
    int32_t rename_ref_count{0};
  };
  void UpsertEntry(Ino ino, const Entry& entry);

  using Map = absl::flat_hash_map<Ino, Entry>;

  constexpr static size_t kShardNum = 32;
  utils::Shards<Map, kShardNum> shard_map_;

  // metrics
  bvar::Adder<uint64_t> total_count_{"meta_parent_memo_total_count"};
};

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_MDS_PARENT_MEMO_H_