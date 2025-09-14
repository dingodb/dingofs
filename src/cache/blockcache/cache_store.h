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

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_CACHE_STORE_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_CACHE_STORE_H_

#include <absl/strings/str_format.h>
#include <absl/strings/str_split.h>
#include <glog/logging.h>

#include "cache/utils/context.h"
#include "common/io_buffer.h"
#include "common/status.h"
#include "dingofs/blockcache.pb.h"
#include "utils/string.h"

namespace dingofs {
namespace cache {

// store type
enum class StoreType : uint8_t {
  kNone = 0,
  kDisk = 1,
  k3FS = 2,
};

// block key
struct BlockKey {
  BlockKey() : fs_id(0), ino(0), id(0), index(0), version(0) {}

  BlockKey(uint64_t fs_id, uint64_t ino, uint64_t id, uint64_t index,
           uint64_t version)
      : fs_id(fs_id), ino(ino), id(id), index(index), version(version) {}

  BlockKey(pb::cache::BlockKey pb)
      : fs_id(pb.fs_id()),
        ino(pb.ino()),
        id(pb.id()),
        index(pb.index()),
        version(pb.version()) {}

  std::string Filename() const {
    return absl::StrFormat("%llu_%llu_%llu_%llu_%llu", fs_id, ino, id, index,
                           version);
  }

  std::string StoreKey() const {
    return absl::StrFormat("blocks/%llu/%llu/%s", id / 1000 / 1000, id / 1000,
                           Filename());
  }

  pb::cache::BlockKey ToPB() const {
    pb::cache::BlockKey pb;
    pb.set_fs_id(fs_id);
    pb.set_ino(ino);
    pb.set_id(id);
    pb.set_index(index);
    pb.set_version(version);
    return pb;
  }

  bool ParseFromFilename(const std::string_view& filename) {
    auto strs = absl::StrSplit(filename, "_");
    return utils::Strs2Ints(strs, {&fs_id, &ino, &id, &index, &version});
  }

  uint64_t fs_id;    // filesystem id
  uint64_t ino;      // inode id
  uint64_t id;       // chunkid
  uint64_t index;    // block index (offset/chunkSize)
  uint64_t version;  // compaction version
};

// block
struct Block {
  Block() = default;
  Block(IOBuffer buffer) : buffer(buffer), size(buffer.Size()) {}
  Block(const char* data, size_t size) : buffer(data, size), size(size) {}

  IOBuffer buffer;
  size_t size{0};
};

// block context
struct BlockContext {
  enum BlockFrom : uint8_t {
    kFromWriteback = 0,
    kFromReload = 1,
    kFromUnknown = 2,
  };

  BlockContext() : from(kFromUnknown), store_id("") {}

  BlockContext(BlockFrom from) : from(from), store_id("") {}

  BlockContext(BlockFrom from, const std::string& store_id)
      : from(from), store_id(store_id) {
    if (!store_id.empty()) {  // Only for block which from reload
      CHECK(from == BlockContext::kFromReload);
    }
  }

  BlockFrom from;
  std::string store_id;  // Specified store id which this block real stored in
                         // (for disk cache group changed)
};

// cache store
class CacheStore {
 public:
  struct StageOption {
    BlockContext block_ctx;
  };

  struct RemoveStageOption {
    BlockContext block_ctx;
  };

  struct CacheOption {};

  struct LoadOption {
    BlockContext block_ctx;
  };

  using UploadFunc = std::function<void(ContextSPtr ctx, const BlockKey& key,
                                        size_t length, BlockContext block_ctx)>;

  virtual ~CacheStore() = default;

  virtual Status Start(UploadFunc uploader) = 0;
  virtual Status Shutdown() = 0;

  virtual Status Stage(ContextSPtr ctx, const BlockKey& key, const Block& block,
                       StageOption option = StageOption()) = 0;
  virtual Status RemoveStage(
      ContextSPtr ctx, const BlockKey& key,
      RemoveStageOption option = RemoveStageOption()) = 0;
  virtual Status Cache(ContextSPtr ctx, const BlockKey& key, const Block& block,
                       CacheOption option = CacheOption()) = 0;
  virtual Status Load(ContextSPtr ctx, const BlockKey& key, off_t offset,
                      size_t length, IOBuffer* buffer,
                      LoadOption option = LoadOption()) = 0;

  virtual std::string Id() const = 0;
  virtual bool IsRunning() const = 0;
  virtual bool IsCached(const BlockKey& key) const = 0;
  virtual bool IsFull(const BlockKey& key) const = 0;
};

using CacheStoreSPtr = std::shared_ptr<CacheStore>;
using CacheStoreUPtr = std::unique_ptr<CacheStore>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_CACHE_STORE_H_
