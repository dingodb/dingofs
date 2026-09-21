// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
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

#include <atomic>
#include <chrono>
#include <future>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "mds/common/crontab.h"

namespace dingofs {
namespace mds {
namespace unit_test {

namespace {

using std::chrono::milliseconds;
using std::chrono::steady_clock;

// Poll a predicate until it returns true or `budget` elapses. Returns the last
// evaluated value.
template <typename F>
bool WaitFor(F pred, milliseconds budget = milliseconds(2000)) {
  auto deadline = steady_clock::now() + budget;
  while (!pred()) {
    if (steady_clock::now() >= deadline) return pred();
    std::this_thread::sleep_for(milliseconds(1));
  }
  return true;
}

CrontabConfig MakeConfig(std::string name, uint32_t interval_ms, bool async,
                         std::function<void()> callback, uint32_t max_times = 0,
                         bool immediately = false) {
  CrontabConfig cfg;
  cfg.name = std::move(name);
  cfg.interval_ms = interval_ms;
  cfg.async = async;
  cfg.callback = std::move(callback);
  cfg.max_times = max_times;
  cfg.immediately = immediately;
  return cfg;
}

}  // namespace

TEST(CrontabTest, ImmediateFiresWithoutWaitingInterval) {
  std::atomic<int> hits{0};
  CrontabManager mgr;
  std::vector<CrontabConfig> cfgs;
  cfgs.push_back(MakeConfig(
      "task", /*interval_ms=*/3600'000, /*async=*/false, [&]() { ++hits; },
      /*max_times=*/1,
      /*immediately=*/true));
  mgr.AddCrontab(std::move(cfgs));
  // The interval is an hour; the callback must have fired without waiting it.
  ASSERT_TRUE(WaitFor([&] { return hits.load() == 1; }));
}

TEST(CrontabTest, NonImmediateFiresAfterInterval) {
  std::atomic<int> hits{0};
  CrontabManager mgr;
  std::vector<CrontabConfig> cfgs;
  cfgs.push_back(MakeConfig("task", /*interval_ms=*/20, /*async=*/false,
                            [&]() { ++hits; }));
  auto start = steady_clock::now();
  mgr.AddCrontab(std::move(cfgs));
  ASSERT_TRUE(WaitFor([&] { return hits.load() >= 1; }));
  // First tick must have waited roughly `interval` before firing.
  EXPECT_GE(steady_clock::now() - start, milliseconds(15));
}

TEST(CrontabTest, MaxTimesLimitsInvocations) {
  std::atomic<int> hits{0};
  CrontabManager mgr;
  std::vector<CrontabConfig> cfgs;
  cfgs.push_back(MakeConfig(
      "task", /*interval_ms=*/5, /*async=*/false, [&]() { ++hits; },
      /*max_times=*/3));
  mgr.AddCrontab(std::move(cfgs));
  ASSERT_TRUE(WaitFor([&] { return hits.load() == 3; }));
  // Give the timer plenty of time to over-fire if the limit is broken.
  std::this_thread::sleep_for(milliseconds(50));
  EXPECT_EQ(hits.load(), 3);
}

TEST(CrontabTest, ConcurrentStopsWaitForRunningCallback) {
  std::promise<void> entered;
  std::promise<void> release;
  auto released = release.get_future().share();
  std::atomic<bool> completed{false};
  CrontabManager mgr;
  mgr.AddCrontab({MakeConfig(
      "task", 1, true,
      [&]() {
        entered.set_value();
        released.wait();
        completed = true;
      },
      1, true)});
  auto entered_status = entered.get_future().wait_for(milliseconds(2000));
  if (entered_status != std::future_status::ready) {
    release.set_value();
    FAIL() << "callback did not start";
  }

  std::promise<void> first_entered, second_entered;
  auto first = std::async(std::launch::async, [&] {
    first_entered.set_value();
    mgr.Stop();
    return completed.load();
  });
  auto second = std::async(std::launch::async, [&] {
    second_entered.set_value();
    mgr.Stop();
    return completed.load();
  });
  first_entered.get_future().wait();
  second_entered.get_future().wait();
  const auto first_status = first.wait_for(milliseconds(50));
  const auto second_status = second.wait_for(milliseconds(50));
  release.set_value();
  EXPECT_TRUE(first.get())
      << "first Stop returned before the callback completed";
  EXPECT_TRUE(second.get())
      << "second Stop returned before the callback completed";
  EXPECT_EQ(first_status, std::future_status::timeout);
  EXPECT_EQ(second_status, std::future_status::timeout);
}

TEST(CrontabTest, StopCancelsPendingTimer) {
  std::atomic<int> hits{0};
  CrontabManager mgr;
  std::vector<CrontabConfig> cfgs;
  cfgs.push_back(MakeConfig("task", /*interval_ms=*/500, /*async=*/false,
                            [&]() { ++hits; }));
  mgr.AddCrontab(std::move(cfgs));
  // Timer is armed but shouldn't have fired yet.
  mgr.Stop();
  // Wait past when the timer would have fired had Stop not cancelled it.
  std::this_thread::sleep_for(milliseconds(600));
  EXPECT_EQ(hits.load(), 0);
}

TEST(CrontabTest, StopIsIdempotent) {
  std::atomic<int> hits{0};
  CrontabManager mgr;
  std::vector<CrontabConfig> cfgs;
  cfgs.push_back(MakeConfig(
      "task", /*interval_ms=*/1, /*async=*/false, [&]() { ++hits; },
      /*max_times=*/0,
      /*immediately=*/true));
  mgr.AddCrontab(std::move(cfgs));
  ASSERT_TRUE(WaitFor([&] { return hits.load() >= 1; }));
  mgr.Stop();
  int after_stop = hits.load();
  mgr.Stop();  // Second call must not crash or double-drain.
  std::this_thread::sleep_for(milliseconds(20));
  EXPECT_EQ(hits.load(), after_stop);
}

TEST(CrontabTest, AddCrontabAfterStopIsNoOp) {
  std::atomic<int> hits{0};
  CrontabManager mgr;
  mgr.Stop();
  std::vector<CrontabConfig> cfgs;
  cfgs.push_back(MakeConfig(
      "task", /*interval_ms=*/1, /*async=*/false, [&]() { ++hits; },
      /*max_times=*/0,
      /*immediately=*/true));
  mgr.AddCrontab(std::move(cfgs));
  std::this_thread::sleep_for(milliseconds(30));
  EXPECT_EQ(hits.load(), 0);
  Json::Value view(Json::arrayValue);
  mgr.DescribeByJson(view);
  EXPECT_EQ(view.size(), 0u);
}

TEST(CrontabTest, DescribeByJsonEnumeratesTasks) {
  std::atomic<int> a{0}, b{0};
  CrontabManager mgr;
  std::vector<CrontabConfig> cfgs;
  cfgs.push_back(MakeConfig(
      "alpha", /*interval_ms=*/1000, /*async=*/true, [&]() { ++a; },
      /*max_times=*/7));
  cfgs.push_back(MakeConfig(
      "beta", /*interval_ms=*/2500, /*async=*/false, [&]() { ++b; },
      /*max_times=*/0,
      /*immediately=*/true));
  mgr.AddCrontab(std::move(cfgs));

  Json::Value view(Json::arrayValue);
  mgr.DescribeByJson(view);
  ASSERT_EQ(view.size(), 2u);

  // Ordering is insertion order.
  EXPECT_EQ(view[0]["name"].asString(), "alpha");
  EXPECT_EQ(view[0]["interval_ms"].asInt64(), 1000);
  EXPECT_EQ(view[0]["max_times"].asUInt(), 7u);
  EXPECT_EQ(view[0]["immediately"].asBool(), false);
  EXPECT_EQ(view[0]["pause"].asBool(), false);

  EXPECT_EQ(view[1]["name"].asString(), "beta");
  EXPECT_EQ(view[1]["interval_ms"].asInt64(), 2500);
  EXPECT_EQ(view[1]["max_times"].asUInt(), 0u);
  EXPECT_EQ(view[1]["immediately"].asBool(), true);
}

TEST(CrontabTest, DestructorDrainsWithoutUseAfterFree) {
  std::atomic<int> hits{0};
  {
    CrontabManager mgr;
    std::vector<CrontabConfig> cfgs;
    // Interval short enough that timers keep re-arming during the sleep;
    // ~mgr must Stop() and drain them before the callback capture goes away.
    cfgs.push_back(MakeConfig(
        "task", /*interval_ms=*/1, /*async=*/true, [&]() { ++hits; },
        /*max_times=*/0,
        /*immediately=*/true));
    mgr.AddCrontab(std::move(cfgs));
    ASSERT_TRUE(WaitFor([&] { return hits.load() >= 5; }));
  }
  // If drain missed a callback the process would have already crashed or
  // continued incrementing `hits` after the manager's teardown. Take a nap
  // and confirm the counter is stable.
  int snapshot = hits.load();
  std::this_thread::sleep_for(milliseconds(20));
  EXPECT_EQ(hits.load(), snapshot);
}

TEST(CrontabTest, MultipleTasksScheduleIndependently) {
  std::atomic<int> fast{0}, slow{0};
  CrontabManager mgr;
  std::vector<CrontabConfig> cfgs;
  cfgs.push_back(MakeConfig(
      "fast", /*interval_ms=*/2, /*async=*/false, [&]() { ++fast; },
      /*max_times=*/10));
  cfgs.push_back(MakeConfig(
      "slow", /*interval_ms=*/100, /*async=*/false, [&]() { ++slow; },
      /*max_times=*/1));
  mgr.AddCrontab(std::move(cfgs));
  ASSERT_TRUE(WaitFor([&] { return fast.load() == 10 && slow.load() == 1; },
                      milliseconds(3000)));
  EXPECT_EQ(fast.load(), 10);
  EXPECT_EQ(slow.load(), 1);
}

TEST(CrontabTest, ThrowingCallbackCountsTowardLimit) {
  std::atomic<int> hits{0};
  Crontab task(1, MakeConfig(
                      "throwing", 1, true,
                      [&]() {
                        ++hits;
                        throw std::runtime_error("callback failed");
                      },
                      3, true));
  task.Launch();
  const bool reached_limit = WaitFor([&] { return hits.load() >= 3; });
  std::this_thread::sleep_for(milliseconds(50));
  task.Stop();
  task.Join();
  EXPECT_TRUE(reached_limit);
  EXPECT_EQ(hits.load(), 3);
  Json::Value view;
  task.DescribeByJson(view);
  EXPECT_EQ(view["run_count"].asUInt(), 3u);
}

TEST(CrontabTest, AsyncSchedulesWhilePreviousInvocationIsBlocked) {
  std::promise<void> release, second_entered;
  auto released = release.get_future().share();
  std::atomic<int> calls{0};
  CrontabManager manager;
  manager.AddCrontab({MakeConfig(
      "overlap", 20, true,
      [&]() {
        if (++calls == 2) second_entered.set_value();
        released.wait();
      },
      2, true)});
  const auto second_status =
      second_entered.get_future().wait_for(milliseconds(2000));
  // Both invocations remain blocked; max_times must prevent a third.
  std::this_thread::sleep_for(milliseconds(50));
  const int before_release = calls.load();
  release.set_value();
  manager.Stop();
  EXPECT_EQ(second_status, std::future_status::ready);
  EXPECT_EQ(before_release, 2);
  EXPECT_EQ(calls.load(), 2);
}

TEST(CrontabTest, StopClosesEveryTaskBeforeWaitingForCallbacks) {
  std::promise<void> entered, release;
  auto released = release.get_future().share();
  std::atomic<int> later_calls{0};
  CrontabManager manager;
  manager.AddCrontab(
      {MakeConfig(
           "blocked", 3600000, true,
           [&]() {
             entered.set_value();
             released.wait();
           },
           1, true),
       MakeConfig("later", 3600000, true, [&]() { ++later_calls; })});
  if (entered.get_future().wait_for(milliseconds(2000)) !=
      std::future_status::ready) {
    release.set_value();
    FAIL() << "callback did not start";
  }
  auto stopped = std::async(std::launch::async, [&] { manager.Stop(); });
  // The public task view must report both tasks closed while Stop is blocked.
  const bool all_closed = WaitFor([&] {
    Json::Value view(Json::arrayValue);
    manager.DescribeByJson(view);
    return view.size() == 2 && view[0]["pause"].asBool() &&
           view[1]["pause"].asBool();
  });
  const auto stop_status = stopped.wait_for(milliseconds(0));
  release.set_value();
  stopped.get();
  EXPECT_TRUE(all_closed);
  EXPECT_EQ(stop_status, std::future_status::timeout);
  EXPECT_EQ(later_calls.load(), 0);
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
