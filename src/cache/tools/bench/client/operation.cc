/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

#include "cache/tools/bench/client/operation.h"

#include <glog/logging.h>

#include <algorithm>
#include <cstring>
#include <utility>

#include "cache/iutil/string_util.h"
#include "common/block/block_handle.h"

namespace dingofs {
namespace cache {
namespace bench {
namespace client {
namespace {

constexpr uint64_t kPageSize = 64 * 1024;

IOBuffer NewPayload(uint64_t size) {
  butil::IOBuf pages;
  auto remaining = size;
  while (remaining > 0) {
    auto page_size = std::min(kPageSize, remaining);
    char* data = new char[page_size];
    std::memset(data, 0, page_size);
    pages.append_user_data(data, page_size, iutil::DeleteBuffer);
    remaining -= page_size;
  }
  return IOBuffer(pages);
}

IOBuffer NewReadBuffer(uint64_t size) {
  char* data = new char[size];
  IOBuffer buffer;
  buffer.AppendUserData(data, size, iutil::DeleteBuffer);
  return buffer;
}

class PutOperation final : public Operation {
 public:
  PutOperation(BlockCacheSPtr block_cache, Options options)
      : block_cache_(std::move(block_cache)),
        options_(options),
        payload_(NewPayload(options_.block_size)) {}

  void Issue(const BlockKey& key, AsyncCallback cb) override {
    PutOption option;
    option.writeback = options_.writeback;
    BlockHandle handle(options_.fs_id, key);
    block_cache_->AsyncPut(handle, payload_, std::move(cb), option);
  }
  uint64_t BytesPerOp() const override { return options_.block_size; }

 private:
  BlockCacheSPtr block_cache_;
  Options options_;
  IOBuffer payload_;
};

class RangeOperation final : public Operation {
 public:
  RangeOperation(BlockCacheSPtr block_cache, Options options)
      : block_cache_(std::move(block_cache)), options_(options) {}

  void Issue(const BlockKey& key, AsyncCallback cb) override {
    auto buffer =
        std::make_shared<IOBuffer>(NewReadBuffer(options_.range_length));
    RangeOption option;
    option.retrieve_storage = options_.retrieve_storage;
    option.block_whole_length = options_.block_size;
    BlockHandle handle(options_.fs_id, key);
    auto* raw = buffer.get();
    block_cache_->AsyncRange(
        handle, options_.range_offset, options_.range_length, raw,
        [buffer, cb = std::move(cb)](Status status) { cb(status); }, option);
  }
  uint64_t BytesPerOp() const override { return options_.range_length; }

 private:
  BlockCacheSPtr block_cache_;
  Options options_;
};

}  // namespace

OperationUPtr NewOperation(BlockCacheSPtr block_cache, const Options& options) {
  switch (options.operation) {
    case OperationType::kPut:
      return std::make_unique<PutOperation>(std::move(block_cache), options);
    case OperationType::kRange:
      return std::make_unique<RangeOperation>(std::move(block_cache), options);
  }
  CHECK(false) << "unknown operation";
  return nullptr;
}

}  // namespace client
}  // namespace bench
}  // namespace cache
}  // namespace dingofs
