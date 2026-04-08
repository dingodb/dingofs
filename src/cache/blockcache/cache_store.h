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

#include <glog/logging.h>
#include <json/value.h>

#include <ostream>

#include "cache/common/context.h"
#include "common/block/block_context.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

// store type
enum class StoreType : uint8_t {
  kNone = 0,
  kDisk = 1,
  k3FS = 2,
};

// block
struct Block {
  Block() = default;
  Block(const IOBuffer& buffer) : buffer(buffer), size(buffer.Size()) {}
  Block(IOBuffer&& buffer)
      : buffer(std::move(buffer)), size(this->buffer.Size()) {}
  Block(const char* data, size_t size) : buffer(data, size), size(size) {}

  IOBuffer buffer;
  size_t size{0};
};

// block context
struct BlockAttr {
  enum BlockFrom : uint8_t {
    kFromWriteback = 0,
    kFromReload = 1,
    kFromUnknown = 2,
  };

  BlockAttr() : from(kFromUnknown), store_id("") {}

  BlockAttr(BlockFrom from) : from(from), store_id("") {}

  BlockAttr(BlockFrom from, const std::string& store_id)
      : from(from), store_id(store_id) {
    if (!store_id.empty()) {  // Only for block which from reload
      CHECK(from == BlockAttr::kFromReload);
    }
  }

  BlockFrom from;
  std::string store_id;  // Specified store id which this block real stored in
                         // (for disk cache group changed)
};

inline std::string BlockFromToString(BlockAttr::BlockFrom from) {
  switch (from) {
    case BlockAttr::kFromWriteback:
      return "writeback";
    case BlockAttr::kFromReload:
      return "reload";
    case BlockAttr::kFromUnknown:
      return "unknown";
    default:
      return "invalid";
  }
}

inline std::ostream& operator<<(std::ostream& os, const BlockAttr& attr) {
  os << "BlockAttr{from=" << BlockFromToString(attr.from)
     << " store_id=" << attr.store_id << "}";
  return os;
}

// cache store
class CacheStore {
 public:
  struct StageOption {
    BlockAttr block_attr;
  };

  struct RemoveStageOption {
    BlockAttr block_attr;
  };

  struct CacheOption {};

  struct LoadOption {
    BlockAttr block_attr;
  };

  using UploadFunc =
      std::function<void(ContextSPtr ctx, const BlockContext& block_ctx,
                         size_t length, BlockAttr block_attr)>;

  virtual ~CacheStore() = default;

  virtual Status Start(UploadFunc uploader) = 0;
  virtual Status Shutdown() = 0;

  virtual Status Stage(ContextSPtr ctx, const BlockContext& block_ctx,
                       const Block& block,
                       StageOption option = StageOption()) = 0;
  virtual Status RemoveStage(
      ContextSPtr ctx, const BlockContext& block_ctx,
      RemoveStageOption option = RemoveStageOption()) = 0;
  virtual Status Cache(ContextSPtr ctx, const BlockContext& block_ctx,
                       const Block& block,
                       CacheOption option = CacheOption()) = 0;
  virtual Status Load(ContextSPtr ctx, const BlockContext& block_ctx,
                      off_t offset, size_t length, IOBuffer* buffer,
                      LoadOption option = LoadOption()) = 0;

  virtual std::string Id() const = 0;
  virtual bool IsRunning() const = 0;
  virtual bool IsCached(const BlockContext& block_ctx) const = 0;
  virtual bool IsFull(const BlockContext& block_ctx) const = 0;
  virtual bool Dump(Json::Value& value) const = 0;
};

using CacheStoreSPtr = std::shared_ptr<CacheStore>;
using CacheStoreUPtr = std::unique_ptr<CacheStore>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_CACHE_STORE_H_
