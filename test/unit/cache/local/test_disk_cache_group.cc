/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2026-06-21
 * Author: AI
 */

#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include "cache/common/slab_pool.h"
#include "cache/local/disk_cache_group.h"
#include "common/block/block_key.h"
#include "common/io_buffer.h"
#include "common/options/cache.h"

namespace dingofs {
namespace cache {

class DiskCacheGroupTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    FLAGS_iodepth = 4;
    ASSERT_TRUE(InitializeGlobalSlabPool().ok());
  }

  void SetUp() override {
    static int seq = 0;
    base_index_ = 700 + (seq++ * 10);
    base_dir_ = "/tmp/dingofs_test_disk_cache_group_" +
                std::to_string(getpid()) + "_" + std::to_string(base_index_);

    options_.push_back(Option(base_index_, "a", 100));
    options_.push_back(Option(base_index_ + 1, "b", 200));
  }

  void TearDown() override { std::filesystem::remove_all(base_dir_); }

  DiskCacheOption Option(uint32_t index, const std::string& name,
                         uint64_t size_mb) const {
    DiskCacheOption option;
    option.cache_index = index;
    option.cache_store = "disk";
    option.cache_dir = base_dir_ + "/" + name;
    option.cache_size_mb = size_mb;
    return option;
  }

  std::vector<uint64_t> CalcWeights(std::vector<DiskCacheOption> options) {
    return DiskCacheGroup::CalcWeights(std::move(options));
  }

  static DiskCacheSPtr NonRoutedStore(DiskCacheGroup& group,
                                      const BlockHandle& handle) {
    auto routed = group.GetStore(handle);
    for (const auto& it : group.stores_) {
      if (it.second != routed) {
        return it.second;
      }
    }
    return nullptr;
  }

  template <typename Pred>
  static bool WaitUntil(Pred pred, int timeout_ms = 3000) {
    for (int waited = 0; waited < timeout_ms; waited += 10) {
      if (pred()) return true;
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
  }

  uint32_t base_index_{0};
  std::string base_dir_;
  std::vector<DiskCacheOption> options_;
};

TEST_F(DiskCacheGroupTest, CalcWeightsNormalizesByGcd) {
  auto weights = CalcWeights(options_);

  ASSERT_EQ(weights.size(), 2u);
  EXPECT_EQ(weights[0], 1u);
  EXPECT_EQ(weights[1], 2u);
}

TEST_F(DiskCacheGroupTest, ShutdownBeforeStartAndDumpEmpty) {
  DiskCacheGroup group(options_);
  Json::Value value;

  EXPECT_EQ(group.Id(), "disk_cache_group");
  EXPECT_FALSE(group.IsRunning());
  EXPECT_TRUE(group.Shutdown().ok());
  EXPECT_TRUE(group.Dump(value));
  ASSERT_TRUE(value["disks"].isArray());
  EXPECT_EQ(value["disks"].size(), 0u);
}

TEST_F(DiskCacheGroupTest, DeleteRoutesToSingleStore) {
  auto saved_ratio = FLAGS_free_space_ratio;
  FLAGS_free_space_ratio = 0.0;

  DiskCacheGroup group(options_);
  auto status = group.Start([](BlockHandle, size_t, BlockAttr) {});
  if (status.IsNotSupport()) {
    GTEST_SKIP() << "io_uring is unavailable in this environment";
  }
  ASSERT_TRUE(status.ok()) << status.ToString();

  BlockHandle handle(1, BlockKey(1, 0, 4));
  const std::string data = "data";
  ASSERT_TRUE(group.Cache(handle, IOBuffer(data.data(), data.size())).ok());
  ASSERT_TRUE(group.IsCached(handle));

  auto other = NonRoutedStore(group, handle);
  ASSERT_NE(other, nullptr);
  ASSERT_TRUE(other->Cache(handle, IOBuffer(data.data(), data.size())).ok());

  ASSERT_TRUE(group.Delete(handle).ok());
  EXPECT_TRUE(WaitUntil([&]() { return !group.IsCached(handle); }));
  EXPECT_TRUE(other->IsCached(handle));

  ASSERT_TRUE(group.Delete(handle).ok());

  ASSERT_TRUE(group.Shutdown().ok());
  FLAGS_free_space_ratio = saved_ratio;
}

}  // namespace cache
}  // namespace dingofs
