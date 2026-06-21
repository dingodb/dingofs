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

#include "cache/cachegroup/heartbeat.h"

#include <gflags/gflags.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

#include "cache/common/mock/mock_mds_client.h"

namespace dingofs {
namespace cache {

using ::testing::_;
using ::testing::Invoke;
using ::testing::NiceMock;

DECLARE_uint32(periodic_heartbeat_interval_s);

class HeartbeatTest : public ::testing::Test {
 protected:
  void SetUp() override {
    saved_interval_ = FLAGS_periodic_heartbeat_interval_s;
    FLAGS_periodic_heartbeat_interval_s = 1;  // smallest practical period
  }
  void TearDown() override {
    FLAGS_periodic_heartbeat_interval_s = saved_interval_;
  }

  template <typename Pred>
  static bool WaitUntil(Pred pred, int timeout_ms = 6000) {
    for (int waited = 0; waited < timeout_ms; waited += 20) {
      if (pred()) return true;
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return pred();
  }

  uint32_t saved_interval_;
};

TEST_F(HeartbeatTest, SendsHeartbeatPeriodically) {
  auto mds = std::make_shared<NiceMock<MockMDSClient>>();
  std::atomic<int> count{0};
  ON_CALL(*mds, Heartbeat(_, _, _))
      .WillByDefault(Invoke([&count](const std::string&, const std::string&,
                                     uint32_t) {
        count.fetch_add(1);
        return Status::OK();
      }));

  Heartbeat heartbeat(mds);
  heartbeat.Start();
  heartbeat.Start();  // idempotent

  EXPECT_TRUE(WaitUntil([&count]() { return count.load() >= 1; }));

  heartbeat.Shutdown();
  heartbeat.Shutdown();  // idempotent
}

TEST_F(HeartbeatTest, SurvivesHeartbeatFailure) {
  auto mds = std::make_shared<NiceMock<MockMDSClient>>();
  std::atomic<int> count{0};
  ON_CALL(*mds, Heartbeat(_, _, _))
      .WillByDefault(Invoke([&count](const std::string&, const std::string&,
                                     uint32_t) {
        count.fetch_add(1);
        return Status::NetError("mds unreachable");
      }));

  Heartbeat heartbeat(mds);
  heartbeat.Start();

  // A failing heartbeat must not stop the periodic loop or crash.
  EXPECT_TRUE(WaitUntil([&count]() { return count.load() >= 1; }));

  heartbeat.Shutdown();
}

}  // namespace cache
}  // namespace dingofs
