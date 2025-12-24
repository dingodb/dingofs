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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_V2_CHUNK_MEMO_H_
#define DINGOFS_SRC_CLIENT_VFS_META_V2_CHUNK_MEMO_H_

#include <sys/types.h>

#include <cstdint>
#include <vector>

#include "absl/container/btree_map.h"
#include "client/vfs/vfs_meta.h"
#include "json/value.h"
#include "mds/common/type.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

class ChunkMemo {
 public:
  ChunkMemo() = default;
  ~ChunkMemo() = default;

  struct Key {
    Ino ino;
    uint32_t chunk_index;

    bool operator<(const Key& other) const {
      if (ino != other.ino) {
        return ino < other.ino;
      }
      return chunk_index < other.chunk_index;
    }
  };

  struct Value {
    uint64_t version;
    uint64_t time_ns;
  };

  void Remember(Ino ino,
                const std::vector<mds::ChunkDescriptor>& chunk_descriptors);
  void Remember(Ino ino, uint32_t chunk_index, uint64_t version);
  void Forget(Ino ino, uint32_t chunk_index);
  void ForgetExpired(uint64_t expire_time_ns);

  uint64_t GetVersion(Ino ino, uint32_t chunk_index);
  std::vector<std::pair<uint32_t, uint64_t>> GetVersion(Ino ino);

  bool Dump(Json::Value& value);
  bool Load(const Json::Value& value);

 private:
  utils::RWLock lock_;

  absl::btree_map<Key, Value> chunk_map_;
};

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_V2_CHUNK_MEMO_H_
