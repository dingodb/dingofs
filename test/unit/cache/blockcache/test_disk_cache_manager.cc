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
 * Created Date: 2026-02-02
 * Author: Wine93
 */
#include <gtest/gtest.h>

#include <filesystem>

#include "cache/blockcache/disk_cache_manager.h"

namespace dingofs {
namespace cache {

class DiskCacheManagerVarsTest : public ::testing::Test {};

TEST_F(DiskCacheManagerVarsTest, Creation) {
  DiskCacheManagerVarsCollector vars(0);

  EXPECT_EQ(vars.used_bytes.get_value(), 0);
  EXPECT_EQ(vars.stage_blocks.get_value(), 0);
  EXPECT_FALSE(vars.stage_full.get_value());
  EXPECT_EQ(vars.cache_blocks.get_value(), 0);
  EXPECT_EQ(vars.cache_bytes.get_value(), 0);
  EXPECT_FALSE(vars.cache_full.get_value());
}

TEST_F(DiskCacheManagerVarsTest, NameGeneration) {
  DiskCacheManagerVarsCollector vars(1);

  EXPECT_EQ(vars.Name("test"), "dingofs_disk_cache_1_test");
}

TEST_F(DiskCacheManagerVarsTest, UpdateStageBlocks) {
  DiskCacheManagerVarsCollector vars(0);

  vars.stage_blocks << 1;
  EXPECT_EQ(vars.stage_blocks.get_value(), 1);

  vars.stage_blocks << 5;
  EXPECT_EQ(vars.stage_blocks.get_value(), 6);

  vars.stage_blocks << -3;
  EXPECT_EQ(vars.stage_blocks.get_value(), 3);
}

TEST_F(DiskCacheManagerVarsTest, UpdateCacheBytes) {
  DiskCacheManagerVarsCollector vars(0);

  vars.cache_bytes << 1024;
  EXPECT_EQ(vars.cache_bytes.get_value(), 1024);

  vars.cache_bytes << 2048;
  EXPECT_EQ(vars.cache_bytes.get_value(), 3072);
}

TEST_F(DiskCacheManagerVarsTest, SetUsedBytes) {
  DiskCacheManagerVarsCollector vars(0);

  vars.used_bytes.set_value(1000);
  EXPECT_EQ(vars.used_bytes.get_value(), 1000);

  vars.used_bytes.set_value(5000);
  EXPECT_EQ(vars.used_bytes.get_value(), 5000);
}

TEST_F(DiskCacheManagerVarsTest, SetStageFull) {
  DiskCacheManagerVarsCollector vars(0);

  EXPECT_FALSE(vars.stage_full.get_value());

  vars.stage_full.set_value(true);
  EXPECT_TRUE(vars.stage_full.get_value());

  vars.stage_full.set_value(false);
  EXPECT_FALSE(vars.stage_full.get_value());
}

TEST_F(DiskCacheManagerVarsTest, SetCacheFull) {
  DiskCacheManagerVarsCollector vars(0);

  EXPECT_FALSE(vars.cache_full.get_value());

  vars.cache_full.set_value(true);
  EXPECT_TRUE(vars.cache_full.get_value());

  vars.cache_full.set_value(false);
  EXPECT_FALSE(vars.cache_full.get_value());
}

TEST_F(DiskCacheManagerVarsTest, Reset) {
  DiskCacheManagerVarsCollector vars(0);

  vars.used_bytes.set_value(1000);
  vars.stage_blocks << 5;
  vars.stage_full.set_value(true);
  vars.cache_blocks << 10;
  vars.cache_bytes << 2048;
  vars.cache_full.set_value(true);

  vars.Reset();

  EXPECT_EQ(vars.used_bytes.get_value(), 0);
  EXPECT_EQ(vars.stage_blocks.get_value(), 0);
  EXPECT_FALSE(vars.stage_full.get_value());
  EXPECT_EQ(vars.cache_blocks.get_value(), 0);
  EXPECT_EQ(vars.cache_bytes.get_value(), 0);
  EXPECT_FALSE(vars.cache_full.get_value());
}

TEST_F(DiskCacheManagerVarsTest, MultipleInstances) {
  DiskCacheManagerVarsCollector vars0(0);
  DiskCacheManagerVarsCollector vars1(1);

  vars0.cache_blocks << 10;
  vars1.cache_blocks << 20;

  EXPECT_EQ(vars0.cache_blocks.get_value(), 10);
  EXPECT_EQ(vars1.cache_blocks.get_value(), 20);
}

class BlockPhaseTest : public ::testing::Test {};

TEST_F(BlockPhaseTest, EnumValues) {
  EXPECT_EQ(static_cast<uint8_t>(BlockPhase::kStaging), 0);
  EXPECT_EQ(static_cast<uint8_t>(BlockPhase::kUploaded), 1);
  EXPECT_EQ(static_cast<uint8_t>(BlockPhase::kCached), 2);
}

TEST_F(BlockPhaseTest, Comparison) {
  BlockPhase phase1 = BlockPhase::kStaging;
  BlockPhase phase2 = BlockPhase::kCached;

  EXPECT_NE(phase1, phase2);
  EXPECT_EQ(phase1, BlockPhase::kStaging);
  EXPECT_EQ(phase2, BlockPhase::kCached);
}

}  // namespace cache
}  // namespace dingofs
