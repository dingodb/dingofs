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

#ifndef DINGOFS_MDS_FILESYSTEM_CHUNK_CACHE_H_
#define DINGOFS_MDS_FILESYSTEM_CHUNK_CACHE_H_

#include <absl/container/btree_map.h>

#include <cstdint>
#include <map>
#include <memory>
#include <vector>

#include "json/value.h"
#include "mds/common/type.h"
#include "utils/concurrent/concurrent.h"
#include "utils/shards.h"

namespace dingofs {
namespace mds {

class ChunkCache;
using ChunkCacheUPtr = std::unique_ptr<ChunkCache>;

// single file chunk cache
class ChunkCache {
 public:
  ChunkCache(uint32_t fs_id);
  ~ChunkCache() = default;

  using ChunkSPtr = std::shared_ptr<ChunkEntry>;

  static ChunkCacheUPtr New(uint32_t fs_id) { return std::make_unique<ChunkCache>(fs_id); }

  // if version is newer then put
  bool PutIf(uint64_t ino, ChunkEntry chunk);
  void Delete(uint64_t ino, uint64_t chunk_index);
  void Delete(uint64_t ino);
  void BatchDeleteIf(const std::function<bool(const Ino&)>& f);

  uint64_t Size();
  ChunkSPtr Get(uint64_t ino, uint64_t chunk_index);
  std::vector<ChunkSPtr> Get(uint64_t ino);

  void Clear();

  void RememberCheckCompact(uint64_t ino, uint64_t chunk_index);
  uint64_t GetLastCheckCompactTimeMs(uint64_t ino, uint64_t chunk_index);

  void DescribeByJson(Json::Value& value);

 private:
  struct Key {
    uint64_t ino{0};
    uint64_t chunk_index{0};

    bool operator<(const Key& other) const {
      if (ino != other.ino) {
        return ino < other.ino;
      }
      return chunk_index < other.chunk_index;
    }
  };

  const uint32_t fs_id_{0};

  // ino/chunk_index -> ChunkEntry
  using Map = absl::btree_map<Key, ChunkSPtr>;
  constexpr static size_t kShardNum = 64;
  utils::Shards<Map, kShardNum> chunk_map_;

  // for remember last check compact time
  utils::RWLock lock_;
  absl::btree_map<Key, uint64_t> check_compact_memo_;

  // statistics
  bvar::Adder<int64_t> count_metrics_;
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_FILESYSTEM_CHUNK_CACHE_H_