/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: Dingofs
 * Created Date: 2024-09-08
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_TEST_CACHE_BLOCKCACHE_BUILDER_BUILDER_H_
#define DINGOFS_TEST_CACHE_BLOCKCACHE_BUILDER_BUILDER_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "blockaccess/mock/mock_accesser.h"
#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/cache_store.h"
#include "cache/blockcache/disk_cache.h"
#include "cache/utils/access_log.h"
#include "options/cache/block_cache.h"
#include "utils/string.h"

namespace dingofs {
namespace cache {

using cache::BlockCacheOption;
using cache::DiskCacheOption;
using utils::GenUuid;

class BlockKeyBuilder {
 public:
  BlockKeyBuilder() = default;

  ~BlockKeyBuilder() = default;

  static BlockKey Build(uint64_t chunk_id) {
    return BlockKey(1, 1, chunk_id, 0, 0);
  }
};

class BlockBuilder {
 public:
  BlockBuilder() = default;

  ~BlockBuilder() = default;

  static Block Build(const std::string& buffer) {
    return Block(buffer.c_str(), buffer.length());
  }
};

class DiskCacheBuilder {
 public:
  using Callback = std::function<void(DiskCacheOption* option)>;

  static DiskCacheOption DefaultOption() {
    auto option = DiskCacheOption();
    option.cache_index() = 0;
    option.cache_dir() = "." + GenUuid();
    option.cache_size_mb() = 1024;
    option.cache_expire_s() = 0;
    return option;
  }

 public:
  DiskCacheBuilder() : option_(DefaultOption()) {}

  DiskCacheBuilder SetOption(Callback callback) {
    callback(&option_);
    return *this;
  }

  std::shared_ptr<DiskCache> Build() {
    system(("mkdir -p " + GetRootDir()).c_str());
    return std::make_shared<DiskCache>(option_);
  }

  void Cleanup() const { system(("rm -r " + GetRootDir()).c_str()); }

  std::string GetRootDir() const { return option_.cache_dir(); }

 private:
  DiskCacheOption option_;
};

class BlockCacheBuilder {
 public:
  using Callback = std::function<void(BlockCacheOption* option)>;

  static BlockCacheOption DefaultOption() {
    option.logging() = false;
    option.upload_stage_workers() = 2;
    option.upload_stage_queue_size() = 10;
    option.disk_cache_options() =
        std::vector<DiskCacheOption>{DiskCacheBuilder::DefaultOption()};
    return option;
  }

 public:
  BlockCacheBuilder() : option_(DefaultOption()) {}

  BlockCacheBuilder SetOption(Callback callback) {
    callback(&option_);
    return *this;
  }

  std::shared_ptr<BlockCache> Build() {
    std::string root_dir = GetRootDir();
    system(("mkdir -p " + root_dir).c_str());

    block_accesser_ = std::make_shared<blockaccess::MockBlockAccesser>();
    auto block_cache =
        std::make_shared<BlockCacheImpl>(option_, block_accesser_.get());

    return block_cache;
  }

  void Cleanup() const { system(("rm -r " + GetRootDir()).c_str()); }

  std::shared_ptr<blockaccess::MockBlockAccesser> GetBlockAccesser() {
    return block_accesser_;
  }

  std::string GetRootDir() const {
    return option_.disk_cache_options()[0].cache_dir();
  }

 private:
  BlockCacheOption option_;
  std::shared_ptr<blockaccess::MockBlockAccesser> block_accesser_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_TEST_CACHE_BLOCKCACHE_BUILDER_BUILDER_H_
