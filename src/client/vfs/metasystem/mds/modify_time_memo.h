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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_MDS_MODIFY_TIME_MEMO_H_
#define DINGOFS_SRC_CLIENT_VFS_META_MDS_MODIFY_TIME_MEMO_H_

#include <sys/types.h>

#include <cstddef>
#include <cstdint>

#include "absl/container/flat_hash_map.h"
#include "client/vfs/vfs_meta.h"
#include "json/value.h"
#include "utils/shards.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

class ModifyTimeMemo {
 public:
  ModifyTimeMemo() = default;
  ~ModifyTimeMemo() = default;

  void Remember(Ino ino);
  void CleanExpired(uint64_t expire_time_s);

  uint64_t Get(Ino ino);
  bool ModifiedSince(Ino ino, uint64_t timestamp);

  void UpdateKernelMtime(Ino ino, uint64_t mtime);
  uint64_t GetKernelMtime(Ino ino);

  size_t Size();
  size_t Bytes();

  void Summary(Json::Value& value);
  bool Dump(Json::Value& value);
  bool Load(const Json::Value& value);

 private:
  void DeleteIf(Ino ino, uint64_t expire_time_ns);

  struct Entry {
    uint64_t last_modify_time_ns{0};
    uint64_t kernel_mtime{0};
  };
  // ino -> modify time ns
  using Map = absl::flat_hash_map<Ino, Entry>;

  constexpr static size_t kShardNum = 128;
  utils::Shards<Map, kShardNum> shard_map_;

  // metric
  bvar::Adder<uint64_t> clean_count_{"meta_modify_time_memo_clean_count"};
};

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_MDS_MODIFY_TIME_MEMO_H_
