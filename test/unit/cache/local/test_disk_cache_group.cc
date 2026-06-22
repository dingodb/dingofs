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

#include <filesystem>
#include <string>
#include <vector>

#include "cache/local/disk_cache_group.h"

namespace dingofs {
namespace cache {

class DiskCacheGroupTest : public ::testing::Test {
 protected:
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

}  // namespace cache
}  // namespace dingofs
