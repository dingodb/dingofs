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

#include "cache/tools/cache_bench/operation.h"

#include <glog/logging.h>

#include <algorithm>
#include <cstring>
#include <utility>

#include "cache/iutil/string_util.h"
#include "common/block/block_handle.h"

namespace dingofs {
namespace cache {
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
        options_(std::move(options)),
        payload_(NewPayload(options_.block_size)) {}

  Status Run(const BlockKey& key) override {
    PutOption option;
    option.writeback = options_.writeback;

    BlockHandle handle(options_.fs_id, key);
    auto status = block_cache_->Put(handle, payload_, option);
    if (!status.ok()) {
      LOG(ERROR) << "Put block (key=" << key.Filename()
                 << ") failed: " << status.ToString();
    }
    return status;
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
      : block_cache_(std::move(block_cache)), options_(std::move(options)) {}

  Status Run(const BlockKey& key) override {
    IOBuffer buffer = NewReadBuffer(options_.range_length);

    RangeOption option;
    option.retrieve_storage = options_.retrieve_storage;
    option.block_whole_length = options_.block_size;

    BlockHandle handle(options_.fs_id, key);
    auto status = block_cache_->Range(handle, options_.range_offset,
                                      options_.range_length, &buffer, option);
    if (!status.ok()) {
      LOG(ERROR) << "Range block (key=" << key.Filename()
                 << ") failed: " << status.ToString();
    }
    return status;
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

  CHECK(false) << "Unknown operation: " << OperationName(options.operation);
  return nullptr;
}

}  // namespace cache
}  // namespace dingofs
