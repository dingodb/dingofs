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
#include <memory>

#include "cache/blockcache/disk_cache.h"
#include "cache/blockcache/disk_health_checker.h"

namespace dingofs {
namespace cache {

class DiskHealthCheckerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = "/tmp/dingofs_test_health_checker_" + std::to_string(getpid());
    std::filesystem::create_directories(test_dir_);

    DiskCacheOption option;
    option.cache_dir = test_dir_;
    option.cache_size_mb = 100;
    option.cache_index = 0;
    layout_ =
        std::make_shared<DiskCacheLayout>(option.cache_index, option.cache_dir);
  }

  void TearDown() override { std::filesystem::remove_all(test_dir_); }

  std::string test_dir_;
  DiskCacheLayoutSPtr layout_;
};

TEST_F(DiskHealthCheckerTest, Construction) {
  DiskHealthChecker checker(layout_);
  EXPECT_TRUE(checker.IsHealthy());
}

TEST_F(DiskHealthCheckerTest, IOSuccessIncrement) {
  DiskHealthChecker checker(layout_);

  checker.IOSuccess();
  checker.IOSuccess();
  checker.IOSuccess();

  EXPECT_TRUE(checker.IsHealthy());
}

TEST_F(DiskHealthCheckerTest, IOErrorIncrement) {
  DiskHealthChecker checker(layout_);

  checker.IOError();
  checker.IOError();

  // Just calling IOError should not immediately make it unhealthy
  // The actual health check is done periodically
  EXPECT_TRUE(checker.IsHealthy());
}

TEST_F(DiskHealthCheckerTest, MixedIOOperations) {
  DiskHealthChecker checker(layout_);

  for (int i = 0; i < 100; ++i) {
    checker.IOSuccess();
  }

  for (int i = 0; i < 5; ++i) {
    checker.IOError();
  }

  // Still healthy with mostly successes
  EXPECT_TRUE(checker.IsHealthy());
}

TEST_F(DiskHealthCheckerTest, StartShutdown) {
  DiskHealthChecker checker(layout_);

  // Create directories needed for the checker
  std::filesystem::create_directories(layout_->GetRootDir());
  std::filesystem::create_directories(layout_->GetStageDir());
  std::filesystem::create_directories(layout_->GetCacheDir());
  std::filesystem::create_directories(layout_->GetProbeDir());

  checker.Start();
  EXPECT_TRUE(checker.IsHealthy());

  checker.Shutdown();
}

}  // namespace cache
}  // namespace dingofs
