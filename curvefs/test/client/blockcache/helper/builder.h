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
 * Project: Curve
 * Created Date: 2024-09-08
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_TEST_CLIENT_BLOCKCACHE_HELPER_BUILDER_H_
#define CURVEFS_TEST_CLIENT_BLOCKCACHE_HELPER_BUILDER_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "curvefs/src/base/filepath/filepath.h"
#include "curvefs/src/base/string/string.h"
#include "curvefs/src/client/blockcache/block_cache.h"
#include "curvefs/src/client/blockcache/cache_store.h"
#include "curvefs/src/client/blockcache/disk_cache.h"
#include "curvefs/src/client/blockcache/log.h"
#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/common/dynamic_config.h"
#include "curvefs/test/client/blockcache/mock/mock_s3_client.h"

namespace curvefs {
namespace client {
namespace blockcache {

USING_FLAG(block_cache_logging);
USING_FLAG(disk_cache_expire_second);
USING_FLAG(disk_cache_free_space_ratio);

using ::curvefs::base::string::GenUuid;
using ::curvefs::base::string::StrJoin;
using ::curvefs::client::common::BlockCacheOption;
using ::curvefs::client::common::DiskCacheOption;

class BlockKeyBuilder {
 public:
  BlockKeyBuilder() = default;

  ~BlockKeyBuilder() = default;

  BlockKey Build(uint64_t chunk_id) { return BlockKey(1, 1, chunk_id, 0, 0); }
};

class BlockBuilder {
 public:
  BlockBuilder() = default;

  ~BlockBuilder() = default;

  Block Build(const std::string& buffer) {
    return Block(buffer.c_str(), buffer.length());
  }
};

class DiskCacheBuilder {
 public:
  using Callback = std::function<void(DiskCacheOption* option)>;

  static DiskCacheOption DefaultOption() {
    FLAGS_block_cache_logging = false;
    FLAGS_disk_cache_free_space_ratio = 0.1;
    FLAGS_disk_cache_expire_second = 0;
    return DiskCacheOption{
        .index = 0,
        .cache_dir = "." + GenUuid(),
        .cache_size = 1073741824,  // 1GiB
    };
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

  void Cleanup() { system(("rm -r " + GetRootDir()).c_str()); }

  std::string GetRootDir() const { return option_.cache_dir; }

 private:
  DiskCacheOption option_;
};

class BlockCacheBuilder {
 public:
  using Callback = std::function<void(BlockCacheOption* option)>;

  static BlockCacheOption DefaultOption() {
    FLAGS_block_cache_logging = false;
    return BlockCacheOption{
        .cache_store = "disk",
        .stage = true,
        .flush_workers = 2,
        .flush_queue_size = 10,
        .upload_stage_workers = 2,
        .upload_stage_queue_size = 10,
        .disk_cache_options =
            std::vector<DiskCacheOption>{DiskCacheBuilder::DefaultOption()},
    };
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

    auto block_cache = std::make_shared<BlockCacheImpl>(option_);
    s3_client_ = std::make_shared<MockS3Client>();
    block_cache->s3_ = s3_client_;

    return block_cache;
  }

  void Cleanup() { system(("rm -r " + GetRootDir()).c_str()); }

  std::shared_ptr<MockS3Client> GetS3Client() { return s3_client_; }

  std::string GetRootDir() const {
    return option_.disk_cache_options[0].cache_dir;
  }

 private:
  BlockCacheOption option_;
  std::shared_ptr<MockS3Client> s3_client_;
};

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_BLOCKCACHE_HELPER_BUILDER_H_
