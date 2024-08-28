/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Project: DingoFS
 * Created Date: 2024-08-19
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_BLOCKCACHE_CACHE_STORE_H_
#define CURVEFS_SRC_CLIENT_BLOCKCACHE_CACHE_STORE_H_

#include <functional>
#include <string>

#include "curvefs/src/base/string/string.h"
#include "curvefs/src/client/blockcache/error.h"

namespace curvefs {
namespace client {
namespace blockcache {

using ::curvefs::base::string::StrFormat;
using ::curvefs::base::string::Strs2Ints;
using ::curvefs::base::string::StrSplit;

struct BlockKey {
  BlockKey() : fs_id(0), ino(0), id(0), index(0), version(0) {}

  BlockKey(uint64_t fs_id, uint64_t ino, uint64_t id, uint64_t index,
           uint64_t version)
      : fs_id(fs_id), ino(ino), id(id), index(index), version(version) {}

  std::string Filename() const {
    return StrFormat("%d_%d_%d_%d_%d", fs_id, ino, id, index, version);
  }

  std::string StoreKey() const {
    return StrFormat("blocks/%d/%d/%s", id / 1000 / 1000, id / 1000,
                     Filename());
  }

  bool ParseFilename(const std::string_view& filename) {
    auto strs = StrSplit(filename, "_");
    return Strs2Ints(strs, {&fs_id, &ino, &id, &index, &version});
  }

  uint64_t fs_id;    // filesystem id
  uint64_t ino;      // inode id
  uint64_t id;       // chunkid
  uint64_t index;    // block index (offset/chunkSize)
  uint64_t version;  // compaction version
};

struct Block {
  Block(const char* data, size_t size) : data(data), size(size) {}

  const char* data;
  size_t size;
};

class BlockReader {
 public:
  virtual BCACHE_ERROR ReadAt(off_t offset, size_t length, char* buffer) = 0;

  virtual void Close() = 0;
};

class CacheStore {
 public:
  using UploadFunc = std::function<void(
      const BlockKey& key, const std::string& stage_path, bool reload)>;

 public:
  virtual BCACHE_ERROR Init(UploadFunc uploader) = 0;

  virtual BCACHE_ERROR Shutdown() = 0;

  virtual BCACHE_ERROR Stage(const BlockKey& key, const Block& block) = 0;

  virtual BCACHE_ERROR RemoveStage(const BlockKey& key) = 0;

  virtual BCACHE_ERROR Cache(const BlockKey& key, const Block& block) = 0;

  virtual BCACHE_ERROR Load(const BlockKey& key,
                            std::shared_ptr<BlockReader>& reader) = 0;

  virtual bool IsCached(const BlockKey& key) = 0;

  virtual std::string Id() = 0;
};

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BLOCKCACHE_CACHE_STORE_H_
