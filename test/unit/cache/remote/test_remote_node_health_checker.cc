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

#include "cache/remote/remote_node_health_checker.h"

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "common/options/cache.h"

namespace dingofs {
namespace cache {

namespace {

template <typename Pred>
bool WaitUntil(Pred pred, int timeout_ms = 3000) {
  for (int waited = 0; waited < timeout_ms; waited += 20) {
    if (pred()) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  return pred();
}

}  // namespace

class RemoteNodeHealthCheckerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    saved_check_duration_ms_ = FLAGS_cache_node_state_check_duration_ms;
    saved_tick_duration_s_ = FLAGS_cache_node_state_tick_duration_s;
    saved_normal2unstable_ = FLAGS_cache_node_state_normal2unstable_error_num;
    saved_unstable2normal_ = FLAGS_cache_node_state_unstable2normal_succ_num;
    saved_unstable2down_s_ = FLAGS_cache_node_state_unstable2down_s;

    FLAGS_cache_node_state_check_duration_ms = 60000;
    FLAGS_cache_node_state_tick_duration_s = 60;
    FLAGS_cache_node_state_normal2unstable_error_num = 0;
    FLAGS_cache_node_state_unstable2normal_succ_num = 0;
    FLAGS_cache_node_state_unstable2down_s = 3600;
  }

  void TearDown() override {
    FLAGS_cache_node_state_check_duration_ms = saved_check_duration_ms_;
    FLAGS_cache_node_state_tick_duration_s = saved_tick_duration_s_;
    FLAGS_cache_node_state_normal2unstable_error_num = saved_normal2unstable_;
    FLAGS_cache_node_state_unstable2normal_succ_num = saved_unstable2normal_;
    FLAGS_cache_node_state_unstable2down_s = saved_unstable2down_s_;
  }

 private:
  uint32_t saved_check_duration_ms_{0};
  uint32_t saved_tick_duration_s_{0};
  uint32_t saved_normal2unstable_{0};
  uint32_t saved_unstable2normal_{0};
  uint32_t saved_unstable2down_s_{0};
};

TEST_F(RemoteNodeHealthCheckerTest, StartAndShutdownIdempotent) {
  RemoteNodeHealthChecker checker("127.0.0.1", 9300);

  checker.Start();
  checker.Start();
  EXPECT_TRUE(checker.IsHealthy());

  checker.Shutdown();
  checker.Shutdown();
}

TEST_F(RemoteNodeHealthCheckerTest, StageIoResultsDriveHealth) {
  RemoteNodeHealthChecker checker("127.0.0.1", 9300);
  checker.Start();

  checker.IOError();
  ASSERT_TRUE(WaitUntil([&]() { return !checker.IsHealthy(); }));

  checker.IOSuccess();
  ASSERT_TRUE(WaitUntil([&]() { return checker.IsHealthy(); }));

  checker.Shutdown();
}

}  // namespace cache
}  // namespace dingofs
