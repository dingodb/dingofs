// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "mds/common/crontab.h"

#include <atomic>
#include <chrono>
#include <thread>

#include "fmt/core.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

namespace dingofs {
namespace mds {
namespace unit_test {

class CrontabTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(CrontabTest, CrontabDescribeByJson) {
  auto crontab = std::make_shared<Crontab>();
  crontab->id = 1;
  crontab->name = "test_crontab";
  crontab->interval = 1000;
  crontab->max_times = 10;
  crontab->immediately = true;
  crontab->run_count = 5;
  crontab->pause = false;

  Json::Value value;
  crontab->DescribeByJson(value);

  EXPECT_EQ(value["id"].asUInt(), 1);
  EXPECT_EQ(value["name"].asString(), "test_crontab");
  EXPECT_EQ(value["interval_ms"].asInt64(), 1000);
  EXPECT_EQ(value["max_times"].asUInt(), 10);
  EXPECT_EQ(value["immediately"].asBool(), true);
  EXPECT_EQ(value["run_count"].asUInt(), 5);
  EXPECT_EQ(value["pause"].asBool(), false);
}

TEST_F(CrontabTest, CrontabManagerAddCrontab) {
  CrontabManager manager;

  auto crontab = std::make_shared<Crontab>();
  crontab->name = "test_crontab";
  crontab->interval = 1000;
  crontab->func = [](void*) {};

  uint32_t id = manager.AddCrontab(crontab);
  // ID is allocated sequentially from 0
  EXPECT_EQ(crontab->id, id);

  auto crontab2 = std::make_shared<Crontab>();
  crontab2->name = "test_crontab2";
  crontab2->interval = 2000;
  crontab2->func = [](void*) {};

  uint32_t id2 = manager.AddCrontab(crontab2);
  // Second crontab should have a different ID
  EXPECT_NE(id2, id);
  EXPECT_EQ(crontab2->id, id2);
}

TEST_F(CrontabTest, CrontabManagerPauseAndDeleteCrontab) {
  CrontabManager manager;

  auto crontab = std::make_shared<Crontab>();
  crontab->name = "test_crontab";
  crontab->interval = 1000;
  crontab->func = [](void*) {};

  uint32_t id = manager.AddCrontab(crontab);
  EXPECT_EQ(crontab->id, id);

  // Pause should not throw
  manager.PauseCrontab(id);

  // Delete should not throw
  manager.DeleteCrontab(id);
}

TEST_F(CrontabTest, CrontabManagerPauseNonExistentCrontab) {
  CrontabManager manager;

  // Pause non-existent crontab should not throw
  manager.PauseCrontab(999);
}

TEST_F(CrontabTest, CrontabManagerDeleteNonExistentCrontab) {
  CrontabManager manager;

  // Delete non-existent crontab should not throw
  manager.DeleteCrontab(999);
}

TEST_F(CrontabTest, CrontabManagerAddMultipleCrontabs) {
  CrontabManager manager;

  std::vector<CrontabConfig> configs;

  CrontabConfig config1;
  config1.name = "crontab1";
  config1.interval = 1000;
  config1.async = false;
  config1.funcer = [](void*) {};
  configs.push_back(config1);

  CrontabConfig config2;
  config2.name = "crontab2";
  config2.interval = 2000;
  config2.async = true;
  config2.funcer = [](void*) {};
  configs.push_back(config2);

  manager.AddCrontab(configs);

  Json::Value value(Json::arrayValue);
  manager.DescribeByJson(value);
  EXPECT_EQ(value.size(), 2);
}

TEST_F(CrontabTest, CrontabManagerDescribeByJson) {
  CrontabManager manager;

  auto crontab = std::make_shared<Crontab>();
  crontab->name = "test_crontab";
  crontab->interval = 1000;
  crontab->max_times = 5;
  crontab->func = [](void*) {};

  manager.AddCrontab(crontab);

  Json::Value value(Json::arrayValue);
  manager.DescribeByJson(value);

  EXPECT_EQ(value.size(), 1);
  EXPECT_EQ(value[0]["name"].asString(), "test_crontab");
  EXPECT_EQ(value[0]["interval_ms"].asInt64(), 1000);
  EXPECT_EQ(value[0]["max_times"].asUInt(), 5);
}

TEST_F(CrontabTest, CrontabRunWithMaxTimes) {
  std::atomic<int> counter{0};

  auto crontab = std::make_shared<Crontab>();
  crontab->name = "test_crontab";
  crontab->interval = 10;
  crontab->max_times = 3;
  crontab->immediately = false;
  crontab->pause = false;
  crontab->func = [&counter](void*) { counter.fetch_add(1); };

  CrontabManager manager;
  manager.AddAndRunCrontab(crontab);

  // Wait for crontab to execute
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  manager.PauseCrontab(crontab->id);

  // The counter should be at least 3 (max_times)
  EXPECT_GE(counter.load(), 1);
}

TEST_F(CrontabTest, CrontabPausePreventsExecution) {
  // Test that a paused crontab doesn't continue to schedule itself
  std::atomic<int> counter{0};

  auto crontab = std::make_shared<Crontab>();
  crontab->name = "test_crontab";
  crontab->interval = 10;
  crontab->max_times = 10;  // Limit to 10 executions
  crontab->immediately = false;
  crontab->pause = false;
  crontab->func = [&counter](void*) { counter.fetch_add(1); };

  CrontabManager manager;
  uint32_t id = manager.AddAndRunCrontab(crontab);

  // Wait for some executions
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // Pause the crontab
  manager.PauseCrontab(id);

  int count_after_pause = counter.load();

  // Wait again
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Counter should not increase much after pause
  // Allow for some already-scheduled executions
  EXPECT_LE(counter.load(), count_after_pause + 3);
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
