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
 * Author: AI
 */

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "cache/blockcache/disk_cache_layout.h"

namespace dingofs {
namespace cache {

class DiskCacheLayoutTest : public ::testing::Test {
 protected:
  void SetUp() override {
    cache_dir_ = "/data/cache/550e8400-e29b-41d4-a716-446655440000";
  }

  std::string cache_dir_;
};

TEST_F(DiskCacheLayoutTest, CacheIndex) {
  DiskCacheLayout layout(0, cache_dir_);
  EXPECT_EQ(layout.CacheIndex(), 0);

  DiskCacheLayout layout2(5, cache_dir_);
  EXPECT_EQ(layout2.CacheIndex(), 5);
}

TEST_F(DiskCacheLayoutTest, GetRootDir) {
  DiskCacheLayout layout(0, cache_dir_);
  EXPECT_EQ(layout.GetRootDir(), cache_dir_);
}

TEST_F(DiskCacheLayoutTest, GetStageDir) {
  DiskCacheLayout layout(0, cache_dir_);
  EXPECT_EQ(layout.GetStageDir(), cache_dir_ + "/stage");
}

TEST_F(DiskCacheLayoutTest, GetCacheDir) {
  DiskCacheLayout layout(0, cache_dir_);
  EXPECT_EQ(layout.GetCacheDir(), cache_dir_ + "/cache");
}

TEST_F(DiskCacheLayoutTest, GetProbeDir) {
  DiskCacheLayout layout(0, cache_dir_);
  EXPECT_EQ(layout.GetProbeDir(), cache_dir_ + "/probe");
}

TEST_F(DiskCacheLayoutTest, GetDetectPath) {
  DiskCacheLayout layout(0, cache_dir_);
  EXPECT_EQ(layout.GetDetectPath(), cache_dir_ + "/.detect");
}

TEST_F(DiskCacheLayoutTest, GetLockPath) {
  DiskCacheLayout layout(0, cache_dir_);
  EXPECT_EQ(layout.GetLockPath(), cache_dir_ + "/.lock");
}

TEST_F(DiskCacheLayoutTest, GetStagePath) {
  DiskCacheLayout layout(0, cache_dir_);
  BlockKey key(1, 2, 3, 4, 5);
  std::string expected = cache_dir_ + "/stage/" + key.StoreKey();
  EXPECT_EQ(layout.GetStagePath(key), expected);
}

TEST_F(DiskCacheLayoutTest, GetCachePath) {
  DiskCacheLayout layout(0, cache_dir_);
  BlockKey key(1, 2, 3, 4, 5);
  std::string expected = cache_dir_ + "/cache/" + key.StoreKey();
  EXPECT_EQ(layout.GetCachePath(key), expected);
}

TEST_F(DiskCacheLayoutTest, RealCacheDir) {
  std::string result =
      RealCacheDir("/data/cache", "550e8400-e29b-41d4-a716-446655440000");
  EXPECT_EQ(result, "/data/cache/550e8400-e29b-41d4-a716-446655440000");
}

TEST_F(DiskCacheLayoutTest, TempFilepath) {
  std::string filepath = "/data/cache/test.txt";
  std::string temp = TempFilepath(filepath);
  EXPECT_TRUE(temp.find(filepath) != std::string::npos);
  EXPECT_TRUE(temp.find(".tmp") != std::string::npos);
}

TEST_F(DiskCacheLayoutTest, IsTempFilepath) {
  EXPECT_TRUE(IsTempFilepath("/data/cache/test.txt.123456.tmp"));
  EXPECT_TRUE(IsTempFilepath("file.tmp"));
  EXPECT_FALSE(IsTempFilepath("/data/cache/test.txt"));
  EXPECT_FALSE(IsTempFilepath("file.txt"));
}

}  // namespace cache
}  // namespace dingofs
