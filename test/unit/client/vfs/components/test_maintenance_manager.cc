/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <future>
#include <mutex>

#include "client/vfs/components/maintenance_manager.h"
#include "common/options/client.h"
#include "common/sync_point.h"
#include "utils/executor/executor.h"
#include "utils/scoped_cleanup.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace {
class ManualMaintenanceExecutor final : public Executor {
  struct Timer {
    int64_t deadline;
    std::function<void()> run;
  };

 public:
  bool Start() override { return true; }
  bool Stop() override { return true; }
  bool Execute(std::function<void()> task) override {
    std::lock_guard<std::mutex> lock(mutex_);
    tasks_.push_back(std::move(task));
    return true;
  }
  bool Schedule(std::function<void()> task, int delay_ms) override {
    std::lock_guard<std::mutex> lock(mutex_);
    timers_.push_back({now_ms_ + delay_ms, std::move(task)});
    return true;
  }
  int ThreadNum() const override { return 1; }
  int TaskNum() const override {
    std::lock_guard<std::mutex> lock(mutex_);
    return tasks_.size() + timers_.size();
  }
  std::string Name() const override { return "maintenance-test"; }
  void RunOne() {
    std::function<void()> task;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!tasks_.empty()) {
        task = std::move(tasks_.front());
        tasks_.pop_front();
      } else {
        CHECK(!timers_.empty());
        auto timer = EarliestTimerLocked();
        now_ms_ = timer->deadline;
        task = std::move(timer->run);
        timers_.erase(timer);
      }
    }
    task();
  }
  void Advance(int milliseconds) {
    int64_t target;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      target = now_ms_ + milliseconds;
    }
    for (;;) {
      std::function<void()> task;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!tasks_.empty()) {
          task = std::move(tasks_.front());
          tasks_.pop_front();
        } else {
          auto timer = EarliestTimerLocked();
          if (timer == timers_.end() || timer->deadline > target) {
            now_ms_ = target;
            return;
          }
          now_ms_ = timer->deadline;
          task = std::move(timer->run);
          timers_.erase(timer);
        }
      }
      task();
    }
  }

 private:
  std::deque<Timer>::iterator EarliestTimerLocked() {
    auto first = timers_.end();
    for (auto it = timers_.begin(); it != timers_.end(); ++it) {
      if (first == timers_.end() || it->deadline < first->deadline) first = it;
    }
    return first;
  }
  mutable std::mutex mutex_;
  std::deque<std::function<void()>> tasks_;
  std::deque<Timer> timers_;
  int64_t now_ms_{0};
};

class ProbeMaintenanceTask final : public MaintenanceTask {
 public:
  ProbeMaintenanceTask(bool block_run = false, bool block_stop = false,
                       int batches = 1)
      : allow_run_(!block_run), allow_stop_(!block_stop), batches_(batches) {}
  bool RunOnce(size_t budget) override {
    EXPECT_GT(budget, 0u);
    std::unique_lock<std::mutex> lock(mutex_);
    ++runs;
    cv_.notify_all();
    cv_.wait(lock, [&] { return allow_run_; });
    return runs < batches_;
  }
  void OnStop() override {
    std::unique_lock<std::mutex> lock(mutex_);
    ++stops;
    cv_.notify_all();
    cv_.wait(lock, [&] { return allow_stop_; });
  }
  bool WaitForRun() {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, std::chrono::seconds(5),
                        [&] { return runs != 0; });
  }
  bool WaitForStop() {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, std::chrono::seconds(5),
                        [&] { return stops != 0; });
  }
  void ReleaseRun() {
    std::lock_guard<std::mutex> lock(mutex_);
    allow_run_ = true;
    cv_.notify_all();
  }
  void ReleaseStop() {
    std::lock_guard<std::mutex> lock(mutex_);
    allow_stop_ = true;
    cv_.notify_all();
  }
  std::atomic<int> runs{0};
  std::atomic<int> stops{0};

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  bool allow_run_;
  bool allow_stop_;
  const int batches_;
};
}  // namespace

TEST(MaintenanceRegistrationTest,
     RegistersAnUnrelatedTaskAndRejectsDuplicates) {
  ManualMaintenanceExecutor executor;
  auto task = std::make_shared<ProbeMaintenanceTask>();
  MaintenanceManager manager;
  ASSERT_TRUE(manager.RegisterTask("other", task, &executor, 1000).ok());
  EXPECT_FALSE(manager
                   .RegisterTask("other",
                                 std::make_shared<ProbeMaintenanceTask>(),
                                 &executor, 1000)
                   .ok());
  EXPECT_FALSE(manager.RegisterTask("alias", task, &executor, 1000).ok());
  ASSERT_TRUE(manager.Start().ok());
  EXPECT_FALSE(manager
                   .RegisterTask("late",
                                 std::make_shared<ProbeMaintenanceTask>(),
                                 &executor, 1000)
                   .ok());
  executor.RunOne();
  executor.RunOne();
  EXPECT_EQ(task->runs, 1);
  ASSERT_TRUE(manager.UnregisterTask("other").ok());
  EXPECT_EQ(task->stops, 1);
  executor.RunOne();
  EXPECT_EQ(task->runs, 1);
  EXPECT_EQ(executor.TaskNum(), 0);
}

TEST(MaintenanceRegistrationTest, UnregisterDrainsAnAlreadyQueuedStep) {
#ifdef NDEBUG
  GTEST_SKIP() << "Queued cancellation boundary requires SyncPoint.";
#else
  ManualMaintenanceExecutor executor;
  auto task = std::make_shared<ProbeMaintenanceTask>();
  MaintenanceManager manager;
  ASSERT_TRUE(manager.RegisterTask("queued", task, &executor, 1000).ok());
  ASSERT_TRUE(manager.Start().ok());
  executor.RunOne();
  std::promise<void> cancelled;
  SyncPoint::GetInstance()->SetCallBack(
      "MaintenanceManager:unregister:cancelled",
      [&](void*) { cancelled.set_value(); });
  SyncPoint::GetInstance()->EnableProcessing();
  auto reset = MakeScopedCleanup([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  auto result = std::async(std::launch::async,
                           [&] { return manager.UnregisterTask("queued"); });
  ASSERT_EQ(cancelled.get_future().wait_for(std::chrono::seconds(5)),
            std::future_status::ready);
  EXPECT_EQ(result.wait_for(std::chrono::milliseconds(0)),
            std::future_status::timeout);
  executor.RunOne();
  EXPECT_TRUE(result.get().ok());
  EXPECT_EQ(task->runs, 0);
  EXPECT_EQ(task->stops, 1);
#endif
}

TEST(MaintenanceRegistrationTest, UnregisterWaitsForTaskLocalCleanup) {
  ManualMaintenanceExecutor executor;
  auto task = std::make_shared<ProbeMaintenanceTask>(false, true);
  MaintenanceManager manager;
  ASSERT_TRUE(manager.RegisterTask("cleanup", task, &executor, 1000).ok());
  ASSERT_TRUE(manager.Start().ok());
  executor.RunOne();
  executor.RunOne();
  auto result = std::async(std::launch::async,
                           [&] { return manager.UnregisterTask("cleanup"); });
  ASSERT_TRUE(task->WaitForStop());
  EXPECT_EQ(result.wait_for(std::chrono::milliseconds(0)),
            std::future_status::timeout);
  task->ReleaseStop();
  EXPECT_TRUE(result.get().ok());
  executor.RunOne();
  EXPECT_EQ(task->runs, 1);
}

TEST(MaintenanceRegistrationTest, StopWaitsForRunningStepBeforeOnStop) {
  ManualMaintenanceExecutor executor;
  auto task = std::make_shared<ProbeMaintenanceTask>(true, false);
  MaintenanceManager manager;
  ASSERT_TRUE(manager.RegisterTask("running", task, &executor, 1000).ok());
  ASSERT_TRUE(manager.Start().ok());
  executor.RunOne();
  auto worker = std::async(std::launch::async, [&] { executor.RunOne(); });
  ASSERT_TRUE(task->WaitForRun());
  auto stopped =
      std::async(std::launch::async, [&] { manager.StopAndDrain(); });
  EXPECT_EQ(stopped.wait_for(std::chrono::milliseconds(20)),
            std::future_status::timeout);
  EXPECT_EQ(task->stops, 0);
  task->ReleaseRun();
  worker.get();
  stopped.get();
  EXPECT_EQ(task->stops, 1);
}

TEST(MaintenanceRegistrationTest,
     ContinuesBatchesWithoutWaitingForAnotherTick) {
  ManualMaintenanceExecutor executor;
  auto task = std::make_shared<ProbeMaintenanceTask>(false, false, 3);
  MaintenanceManager manager;
  ASSERT_TRUE(manager.RegisterTask("batches", task, &executor, 1000).ok());
  ASSERT_TRUE(manager.Start().ok());
  executor.RunOne();
  executor.RunOne();
  executor.RunOne();
  executor.RunOne();
  EXPECT_EQ(task->runs, 3);
  manager.StopAndDrain();
  EXPECT_EQ(task->stops, 1);
}

TEST(MaintenanceRegistrationTest, EachTaskRunsAtItsRegisteredInterval) {
  ManualMaintenanceExecutor executor;
  auto fast = std::make_shared<ProbeMaintenanceTask>();
  auto slow = std::make_shared<ProbeMaintenanceTask>();
  MaintenanceManager manager;
  ASSERT_TRUE(manager.RegisterTask("fast", fast, &executor, 500).ok());
  ASSERT_TRUE(manager.RegisterTask("slow", slow, &executor, 2000).ok());
  ASSERT_TRUE(manager.Start().ok());
  executor.Advance(499);
  EXPECT_EQ(fast->runs, 0);
  EXPECT_EQ(slow->runs, 0);
  executor.Advance(1);
  EXPECT_EQ(fast->runs, 1);
  EXPECT_EQ(slow->runs, 0);
  executor.Advance(1500);
  EXPECT_EQ(fast->runs, 4);
  EXPECT_EQ(slow->runs, 1);
}

TEST(MaintenanceRegistrationTest,
     GlobalStopWaitsForConcurrentUnregisterCleanup) {
  ManualMaintenanceExecutor executor;
  auto task = std::make_shared<ProbeMaintenanceTask>(false, true);
  MaintenanceManager manager;
  ASSERT_TRUE(manager.RegisterTask("cleanup", task, &executor, 1000).ok());
  auto unregister = std::async(
      std::launch::async, [&] { return manager.UnregisterTask("cleanup"); });
  ASSERT_TRUE(task->WaitForStop());
  auto stopped =
      std::async(std::launch::async, [&] { manager.StopAndDrain(); });
  EXPECT_EQ(stopped.wait_for(std::chrono::milliseconds(20)),
            std::future_status::timeout);
  task->ReleaseStop();
  EXPECT_TRUE(unregister.get().ok());
  stopped.get();
  EXPECT_EQ(task->stops, 1);
}

TEST(MaintenanceRegistrationTest, ConcurrentStopWaitsForTheFirstDrain) {
  ManualMaintenanceExecutor executor;
  auto task = std::make_shared<ProbeMaintenanceTask>(false, true);
  MaintenanceManager manager;
  ASSERT_TRUE(manager.RegisterTask("cleanup", task, &executor, 1000).ok());
  auto first = std::async(std::launch::async, [&] { manager.StopAndDrain(); });
  ASSERT_TRUE(task->WaitForStop());
  auto second = std::async(std::launch::async, [&] { manager.StopAndDrain(); });
  EXPECT_EQ(second.wait_for(std::chrono::milliseconds(20)),
            std::future_status::timeout);
  task->ReleaseStop();
  first.get();
  second.get();
  EXPECT_EQ(task->stops, 1);
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
