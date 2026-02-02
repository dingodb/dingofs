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

#include <atomic>
#include <thread>
#include <vector>

#include "cache/iutil/inflight_tracker.h"

namespace dingofs {
namespace cache {
namespace iutil {

class InflightTrackerTest : public ::testing::Test {};

TEST_F(InflightTrackerTest, BasicAddRemove) {
  InflightTracker tracker(10);

  auto status = tracker.Add("task1");
  EXPECT_TRUE(status.ok());

  tracker.Remove("task1");
}

TEST_F(InflightTrackerTest, DuplicateAdd) {
  InflightTracker tracker(10);

  auto status1 = tracker.Add("task1");
  EXPECT_TRUE(status1.ok());

  auto status2 = tracker.Add("task1");
  EXPECT_TRUE(status2.IsExist());

  tracker.Remove("task1");
}

TEST_F(InflightTrackerTest, MultipleTasksUnique) {
  InflightTracker tracker(10);

  EXPECT_TRUE(tracker.Add("task1").ok());
  EXPECT_TRUE(tracker.Add("task2").ok());
  EXPECT_TRUE(tracker.Add("task3").ok());

  EXPECT_TRUE(tracker.Add("task1").IsExist());
  EXPECT_TRUE(tracker.Add("task2").IsExist());
  EXPECT_TRUE(tracker.Add("task3").IsExist());

  tracker.Remove("task1");
  tracker.Remove("task2");
  tracker.Remove("task3");
}

TEST_F(InflightTrackerTest, AddAfterRemove) {
  InflightTracker tracker(10);

  EXPECT_TRUE(tracker.Add("task1").ok());
  tracker.Remove("task1");

  // Should be able to add again after remove
  EXPECT_TRUE(tracker.Add("task1").ok());
  tracker.Remove("task1");
}

TEST_F(InflightTrackerTest, MaxInflightsLimit) {
  InflightTracker tracker(2);

  EXPECT_TRUE(tracker.Add("task1").ok());
  EXPECT_TRUE(tracker.Add("task2").ok());

  // Adding third task should block, test with timeout
  std::atomic<bool> added{false};
  std::thread t([&tracker, &added]() {
    auto status = tracker.Add("task3");
    added = status.ok();
  });

  // Wait a bit to ensure the thread is blocked
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(added.load());

  // Remove one task to unblock
  tracker.Remove("task1");
  t.join();

  EXPECT_TRUE(added.load());

  tracker.Remove("task2");
  tracker.Remove("task3");
}

TEST_F(InflightTrackerTest, ConcurrentAddRemove) {
  InflightTracker tracker(100);
  std::atomic<int> success_count{0};

  auto worker = [&tracker, &success_count](int id) {
    for (int i = 0; i < 50; ++i) {
      std::string key =
          "worker" + std::to_string(id) + "_task" + std::to_string(i);
      auto status = tracker.Add(key);
      if (status.ok()) {
        success_count++;
        // Simulate work
        std::this_thread::sleep_for(std::chrono::microseconds(10));
        tracker.Remove(key);
      }
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < 4; ++i) {
    threads.emplace_back(worker, i);
  }

  for (auto& t : threads) {
    t.join();
  }

  // All adds should succeed since each worker has unique keys
  EXPECT_EQ(success_count.load(), 200);
}

TEST_F(InflightTrackerTest, ZeroInflightsNeverAdd) {
  // With 0 max inflights, Add should always block
  // We can't really test this without timeout, skip for now
}

TEST_F(InflightTrackerTest, SingleInflight) {
  InflightTracker tracker(1);

  EXPECT_TRUE(tracker.Add("task1").ok());

  std::atomic<bool> added{false};
  std::thread t([&tracker, &added]() {
    auto status = tracker.Add("task2");
    added = status.ok();
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(added.load());

  tracker.Remove("task1");
  t.join();

  EXPECT_TRUE(added.load());
  tracker.Remove("task2");
}

}  // namespace iutil
}  // namespace cache
}  // namespace dingofs
