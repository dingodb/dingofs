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

#include <gtest/gtest.h>

#include <filesystem>

#include "cache/blockcache/disk_cache.h"

namespace dingofs {
namespace cache {

class DiskCacheOptionTest : public ::testing::Test {};

TEST_F(DiskCacheOptionTest, DefaultConstructor) {
  DiskCacheOption option;
  // Default values should be set
  EXPECT_EQ(option.cache_index, 0);
  EXPECT_TRUE(option.cache_store.empty());
  EXPECT_TRUE(option.cache_dir.empty());
  EXPECT_EQ(option.cache_size_mb, 0);
}

TEST_F(DiskCacheOptionTest, SetValues) {
  DiskCacheOption option;
  option.cache_index = 1;
  option.cache_store = "disk";
  option.cache_dir = "/tmp/cache";
  option.cache_size_mb = 1024;

  EXPECT_EQ(option.cache_index, 1);
  EXPECT_EQ(option.cache_store, "disk");
  EXPECT_EQ(option.cache_dir, "/tmp/cache");
  EXPECT_EQ(option.cache_size_mb, 1024);
}

class DiskCacheVarsCollectorTest : public ::testing::Test {};

TEST_F(DiskCacheVarsCollectorTest, Construction) {
  DiskCacheVarsCollector vars(0, "/tmp/cache", 100, 0.1);

  EXPECT_EQ(vars.cache_index, 0);
  EXPECT_EQ(vars.capacity.get_value(), 100 * 1024 * 1024);  // MB to bytes
  EXPECT_DOUBLE_EQ(vars.free_space_ratio.get_value(), 0.1);
}

TEST_F(DiskCacheVarsCollectorTest, NameGeneration) {
  DiskCacheVarsCollector vars(1, "/tmp/cache", 100, 0.1);

  EXPECT_EQ(vars.Name("test"), "dingofs_disk_cache_1_test");
  EXPECT_EQ(vars.Name("status"), "dingofs_disk_cache_1_status");
}

TEST_F(DiskCacheVarsCollectorTest, RecordStageSkips) {
  DiskCacheVarsCollector vars(0, "/tmp/cache", 100, 0.1);

  EXPECT_EQ(vars.stage_skips.get_value(), 0);

  vars.stage_skips << 1;
  EXPECT_EQ(vars.stage_skips.get_value(), 1);

  vars.stage_skips << 5;
  EXPECT_EQ(vars.stage_skips.get_value(), 6);
}

TEST_F(DiskCacheVarsCollectorTest, RecordCacheHitsMisses) {
  DiskCacheVarsCollector vars(0, "/tmp/cache", 100, 0.1);

  vars.cache_hits << 10;
  vars.cache_misses << 3;

  EXPECT_EQ(vars.cache_hits.get_value(), 10);
  EXPECT_EQ(vars.cache_misses.get_value(), 3);
}

TEST_F(DiskCacheVarsCollectorTest, RunningStatus) {
  DiskCacheVarsCollector vars(0, "/tmp/cache", 100, 0.1);

  EXPECT_EQ(vars.running_status.get_value(), "down");

  vars.running_status.set_value("up");
  EXPECT_EQ(vars.running_status.get_value(), "up");
}

TEST_F(DiskCacheVarsCollectorTest, Reset) {
  DiskCacheVarsCollector vars(0, "/tmp/cache", 100, 0.1);

  vars.running_status.set_value("up");
  vars.stage_skips << 5;
  vars.cache_hits << 10;
  vars.cache_misses << 3;

  vars.Reset();

  EXPECT_EQ(vars.running_status.get_value(), "down");
  EXPECT_EQ(vars.stage_skips.get_value(), 0);
  EXPECT_EQ(vars.cache_hits.get_value(), 0);
  EXPECT_EQ(vars.cache_misses.get_value(), 0);
}

TEST_F(DiskCacheVarsCollectorTest, DifferentCacheIndices) {
  DiskCacheVarsCollector vars0(0, "/tmp/cache0", 100, 0.1);
  DiskCacheVarsCollector vars1(1, "/tmp/cache1", 200, 0.2);
  DiskCacheVarsCollector vars2(2, "/tmp/cache2", 300, 0.3);

  EXPECT_EQ(vars0.cache_index, 0);
  EXPECT_EQ(vars1.cache_index, 1);
  EXPECT_EQ(vars2.cache_index, 2);

  EXPECT_EQ(vars0.Name("test"), "dingofs_disk_cache_0_test");
  EXPECT_EQ(vars1.Name("test"), "dingofs_disk_cache_1_test");
  EXPECT_EQ(vars2.Name("test"), "dingofs_disk_cache_2_test");
}

class DiskCacheVarsRecordGuardTest : public ::testing::Test {};

TEST_F(DiskCacheVarsRecordGuardTest, LoadHit) {
  DiskCacheVarsCollector vars(0, "/tmp/cache", 100, 0.1);
  Status status = Status::OK();

  {
    DiskCacheVarsRecordGuard guard("Load", status, &vars);
  }

  EXPECT_EQ(vars.cache_hits.get_value(), 1);
  EXPECT_EQ(vars.cache_misses.get_value(), 0);
}

TEST_F(DiskCacheVarsRecordGuardTest, LoadMiss) {
  DiskCacheVarsCollector vars(0, "/tmp/cache", 100, 0.1);
  Status status = Status::NotFound("not found");

  {
    DiskCacheVarsRecordGuard guard("Load", status, &vars);
  }

  EXPECT_EQ(vars.cache_hits.get_value(), 0);
  EXPECT_EQ(vars.cache_misses.get_value(), 1);
}

TEST_F(DiskCacheVarsRecordGuardTest, StageSkip) {
  DiskCacheVarsCollector vars(0, "/tmp/cache", 100, 0.1);
  Status status = Status::Internal("disk full");

  {
    DiskCacheVarsRecordGuard guard("Stage", status, &vars);
  }

  EXPECT_EQ(vars.stage_skips.get_value(), 1);
}

TEST_F(DiskCacheVarsRecordGuardTest, StageSuccess) {
  DiskCacheVarsCollector vars(0, "/tmp/cache", 100, 0.1);
  Status status = Status::OK();

  {
    DiskCacheVarsRecordGuard guard("Stage", status, &vars);
  }

  EXPECT_EQ(vars.stage_skips.get_value(), 0);
}

TEST_F(DiskCacheVarsRecordGuardTest, OtherOperations) {
  DiskCacheVarsCollector vars(0, "/tmp/cache", 100, 0.1);
  Status status = Status::OK();

  {
    DiskCacheVarsRecordGuard guard("Cache", status, &vars);
  }

  // Cache operation should not affect hits/misses or skips
  EXPECT_EQ(vars.cache_hits.get_value(), 0);
  EXPECT_EQ(vars.cache_misses.get_value(), 0);
  EXPECT_EQ(vars.stage_skips.get_value(), 0);
}

}  // namespace cache
}  // namespace dingofs
