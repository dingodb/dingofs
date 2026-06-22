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

#include "cache/local/disk_health_checker.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>

#include "cache/local/disk_cache_layout.h"

namespace dingofs {
namespace cache {

class DiskHealthCheckerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    static int seq = 0;
    cache_index_ = 300 + (seq++);
    root_dir_ = "/tmp/dingofs_test_disk_health_checker_" +
                std::to_string(getpid()) + "_" + std::to_string(cache_index_);
    std::filesystem::create_directories(root_dir_);
    layout_ = std::make_shared<DiskCacheLayout>(cache_index_, root_dir_);
    std::filesystem::create_directories(layout_->GetProbeDir());
  }

  void TearDown() override { std::filesystem::remove_all(root_dir_); }

  int cache_index_;
  std::string root_dir_;
  DiskCacheLayoutSPtr layout_;
};

TEST_F(DiskHealthCheckerTest, StartShutdownAndInitiallyHealthy) {
  DiskHealthChecker checker(layout_);
  EXPECT_TRUE(checker.IsHealthy());

  checker.Start();
  checker.Start();  // idempotent
  EXPECT_TRUE(checker.IsHealthy());

  checker.Shutdown();
  checker.Shutdown();  // idempotent
}

TEST_F(DiskHealthCheckerTest, StaysHealthyWithSuccesses) {
  DiskHealthChecker checker(layout_);
  checker.Start();

  for (int i = 0; i < 20; ++i) {
    checker.IOSuccess();
  }
  // CommitStageIOResult runs every 1s; give it time to fold in the successes.
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));
  EXPECT_TRUE(checker.IsHealthy());

  checker.Shutdown();
}

TEST_F(DiskHealthCheckerTest, BecomesUnhealthyAfterErrors) {
  DiskHealthChecker checker(layout_);
  checker.Start();

  // Default normal2unstable_error_num is 3; report more errors than that. The
  // commit loop (1s) pushes them to the state machine, which transitions to
  // Unstable asynchronously, so it can take a couple of commit cycles before
  // is_healthy_ flips. Poll up to 6s.
  for (int i = 0; i < 10; ++i) {
    checker.IOError();
  }

  bool became_unhealthy = false;
  for (int waited = 0; waited < 6000; waited += 50) {
    if (!checker.IsHealthy()) {
      became_unhealthy = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  EXPECT_TRUE(became_unhealthy);

  checker.Shutdown();
}

// The first PeriodicCheckDisk fires ~disk_state_check_duration_ms (3s) after
// Start; a healthy probe (write+read of the probe file) must keep state normal.
TEST_F(DiskHealthCheckerTest, PeriodicProbeKeepsHealthy) {
  DiskHealthChecker checker(layout_);
  checker.Start();
  std::this_thread::sleep_for(std::chrono::milliseconds(4500));
  EXPECT_TRUE(checker.IsHealthy());
  checker.Shutdown();
}

// Enough errors drive Normal->Unstable, then enough successes recover to Normal.
TEST_F(DiskHealthCheckerTest, RecoversFromErrorsToHealthy) {
  DiskHealthChecker checker(layout_);
  checker.Start();

  for (int i = 0; i < 10; ++i) {
    checker.IOError();
  }
  bool unhealthy = false;
  for (int waited = 0; waited < 6000 && !unhealthy; waited += 50) {
    unhealthy = !checker.IsHealthy();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  ASSERT_TRUE(unhealthy);

  // default unstable2normal_succ_num is 10, so report well above that
  for (int i = 0; i < 50; ++i) {
    checker.IOSuccess();
  }
  bool healthy_again = false;
  for (int waited = 0; waited < 8000 && !healthy_again; waited += 50) {
    healthy_again = checker.IsHealthy();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  EXPECT_TRUE(healthy_again);

  checker.Shutdown();
}

}  // namespace cache
}  // namespace dingofs
