// Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "utils/logclean_manager.h"

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>

#include "common/options/common.h"

DECLARE_bool(log_clean_enable);
DECLARE_int32(log_retention_seconds);
DECLARE_string(log_clean_filter_pattern);

namespace dingofs {
namespace utils {
namespace unit_test {

namespace fs = std::filesystem;

class LogCleanManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    log_dir_ = fs::temp_directory_path() /
              fs::path("dingofs_logclean_test_" +
                       std::to_string(reinterpret_cast<uintptr_t>(this)));
    fs::create_directories(log_dir_);

    saved_enable_ = FLAGS_log_clean_enable;
    saved_retention_ = FLAGS_log_retention_seconds;
    saved_pattern_ = FLAGS_log_clean_filter_pattern;
  }

  void TearDown() override {
    std::error_code ec;
    fs::remove_all(log_dir_, ec);

    FLAGS_log_clean_enable = saved_enable_;
    FLAGS_log_retention_seconds = saved_retention_;
    FLAGS_log_clean_filter_pattern = saved_pattern_;
  }

  fs::path log_dir_;
  bool saved_enable_;
  int32_t saved_retention_;
  std::string saved_pattern_;
};

TEST_F(LogCleanManagerTest, StartAndStopSucceed) {
  LogCleanManager manager(log_dir_.string());

  Status start_status = manager.Start();
  EXPECT_TRUE(start_status.ok());

  Status stop_status = manager.Stop();
  EXPECT_TRUE(stop_status.ok());
}

TEST_F(LogCleanManagerTest, StartTwiceReturnsErrorOnSecondCall) {
  LogCleanManager manager(log_dir_.string());

  ASSERT_TRUE(manager.Start().ok());
  Status second_start = manager.Start();

  EXPECT_FALSE(second_start.ok());

  manager.Stop();
}

TEST_F(LogCleanManagerTest, StopWithoutStartIsNoop) {
  LogCleanManager manager(log_dir_.string());

  Status stop_status = manager.Stop();
  EXPECT_TRUE(stop_status.ok());
}

// Note: LogCleanManager::CleanLogs/GenerateProjectFiles are private and only
// reachable via the scheduled background task, which the current
// implementation delays by a full kCleanIntervalMs (60s) after Start().
// That makes the cleanup body untestable in a reasonable amount of time
// through the public API alone; exercising it would require either a test
// seam (e.g. an injectable clean interval) or a friend test hook, neither of
// which exists today.

}  // namespace unit_test
}  // namespace utils
}  // namespace dingofs
