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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>              // NOLINT
#include <condition_variable>  // NOLINT
#include <future>
#include <memory>
#include <mutex>   // NOLINT
#include <thread>  // NOLINT
#include <utility>
#include <vector>

#include "utils/executor/bthread/bthread_executor.h"
#include "utils/executor/thread/executor_impl.h"

namespace dingofs {
namespace unit_test {

TEST(BthreadExecutorTest, StartStopReportsThreadCount) {
  BthreadExecutor executor(3);

  EXPECT_TRUE(executor.Start());
  EXPECT_EQ(executor.ThreadNum(), 3);
  EXPECT_EQ(executor.TaskNum(), 0);
  EXPECT_EQ(executor.Name(), "BthreadExecutor");

  EXPECT_TRUE(executor.Stop());
  // Stopping an already-stopped executor reports false.
  EXPECT_FALSE(executor.Stop());
}

TEST(BthreadExecutorTest, ExecuteRunsTask) {
  BthreadExecutor executor(2);
  ASSERT_TRUE(executor.Start());

  std::mutex mutex;
  std::condition_variable cv;
  bool ran = false;

  EXPECT_TRUE(executor.Execute([&] {
    std::lock_guard<std::mutex> lg(mutex);
    ran = true;
    cv.notify_one();
  }));

  std::unique_lock<std::mutex> lk(mutex);
  cv.wait_for(lk, std::chrono::seconds(2), [&] { return ran; });
  EXPECT_TRUE(ran);

  executor.Stop();
}

TEST(BthreadExecutorTest, ScheduleRunsTaskAfterDelay) {
  BthreadExecutor executor(2);
  ASSERT_TRUE(executor.Start());

  std::mutex mutex;
  std::condition_variable cv;
  bool ran = false;

  EXPECT_TRUE(executor.Schedule(
      [&] {
        std::lock_guard<std::mutex> lg(mutex);
        ran = true;
        cv.notify_one();
      },
      10));

  std::unique_lock<std::mutex> lk(mutex);
  cv.wait_for(lk, std::chrono::seconds(2), [&] { return ran; });
  EXPECT_TRUE(ran);

  executor.Stop();
}

TEST(BthreadExecutorTest, StopDestroysPendingScheduledTask) {
  BthreadExecutor executor(1);
  ASSERT_TRUE(executor.Start());

  std::atomic<bool> ran{false};
  auto lifetime = std::make_shared<int>(1);
  std::weak_ptr<int> weak_lifetime = lifetime;
  ASSERT_TRUE(executor.Schedule(
      [lifetime, &ran] { ran.store(true, std::memory_order_release); },
      60 * 60 * 1000));
  lifetime.reset();

  ASSERT_FALSE(weak_lifetime.expired());
  ASSERT_TRUE(executor.Stop());
  EXPECT_FALSE(ran.load(std::memory_order_acquire));
  EXPECT_TRUE(weak_lifetime.expired());
}

TEST(BthreadExecutorTest, ScheduleAfterStopRejectsAndDestroysTask) {
  BthreadExecutor executor(1);
  ASSERT_TRUE(executor.Start());
  ASSERT_TRUE(executor.Stop());

  std::atomic<bool> ran{false};
  auto lifetime = std::make_shared<int>(1);
  std::weak_ptr<int> weak_lifetime = lifetime;
  std::function<void()> task = [lifetime, &ran] {
    ran.store(true, std::memory_order_release);
  };
  lifetime.reset();

  EXPECT_FALSE(executor.Schedule(std::move(task), 1));
  EXPECT_FALSE(ran.load(std::memory_order_acquire));
  EXPECT_TRUE(weak_lifetime.expired());
}

namespace {

// A timed-out Stop must not hang the suite or retain references to test locals.
// On that failure path the thread retains the executor until Stop finishes.
class AsyncStop {
 public:
  explicit AsyncStop(std::shared_ptr<ExecutorImpl> executor,
                     std::shared_future<void> begin = {}) {
    std::packaged_task<bool()> task([executor, begin] {
      if (begin.valid()) {
        begin.wait();
      }
      return executor->Stop();
    });
    result_ = task.get_future();
    thread_ = std::thread(std::move(task));
  }

  ~AsyncStop() {
    if (thread_.joinable()) {
      thread_.detach();
    }
  }

  bool IsReady(std::chrono::milliseconds timeout) {
    return result_.wait_for(timeout) == std::future_status::ready;
  }

  bool Finish() {
    if (!IsReady(std::chrono::seconds(5))) {
      return false;
    }
    thread_.join();
    return result_.get();
  }

 private:
  std::future<bool> result_;
  std::thread thread_;
};

}  // namespace

TEST(ExecutorImplTest, StartStopReportsThreadCount) {
  ExecutorImpl executor("unit_test_exec", 3);

  ASSERT_TRUE(executor.Start());
  EXPECT_FALSE(executor.Start());
  EXPECT_EQ(executor.ThreadNum(), 3);

  EXPECT_TRUE(executor.Stop());
  EXPECT_FALSE(executor.Stop());
}

TEST(ExecutorImplTest, StopDrainsImmediateQueueAfterBlockedWorker) {
  auto executor = std::make_shared<ExecutorImpl>("unit_test_exec", 1);
  ASSERT_TRUE(executor->Start());

  auto entered = std::make_shared<std::promise<void>>();
  auto entered_future = entered->get_future();
  std::promise<void> release;
  auto released = release.get_future().share();
  auto completed = std::make_shared<std::atomic<int>>(0);
  EXPECT_TRUE(executor->Execute([entered, released] {
    entered->set_value();
    released.wait();
  }));
  // No fatal assertions until the blocker has been released.
  EXPECT_EQ(entered_future.wait_for(std::chrono::seconds(5)),
            std::future_status::ready);
  constexpr int kQueuedTasks = 32;
  for (int i = 0; i < kQueuedTasks; ++i) {
    EXPECT_TRUE(executor->Execute([completed] { ++*completed; }));
  }
  EXPECT_EQ(executor->TaskNum(), kQueuedTasks);

  AsyncStop stop(executor);
  EXPECT_FALSE(stop.IsReady(std::chrono::milliseconds(20)));
  EXPECT_EQ(completed->load(), 0);
  release.set_value();
  EXPECT_TRUE(stop.Finish());
  EXPECT_EQ(completed->load(), kQueuedTasks);
}

TEST(ExecutorImplTest, DueTasksWaitForCpuWorker) {
  auto executor = std::make_shared<ExecutorImpl>("unit_test_exec", 1);
  ASSERT_TRUE(executor->Start());

  auto entered = std::make_shared<std::promise<std::thread::id>>();
  auto entered_future = entered->get_future();
  std::promise<void> release;
  auto released = release.get_future().share();
  EXPECT_TRUE(executor->Execute([entered, released] {
    entered->set_value(std::this_thread::get_id());
    released.wait();
  }));
  const bool worker_entered =
      entered_future.wait_for(std::chrono::seconds(5)) ==
      std::future_status::ready;
  EXPECT_TRUE(worker_entered);
  const auto worker_id =
      worker_entered ? entered_future.get() : std::thread::id{};

  std::vector<std::future<std::thread::id>> callbacks;
  for (int delay_ms : {-1, 0, 10}) {
    auto completed = std::make_shared<std::promise<std::thread::id>>();
    callbacks.push_back(completed->get_future());
    EXPECT_TRUE(executor->Schedule(
        [completed] { completed->set_value(std::this_thread::get_id()); },
        delay_ms));
  }
  // Wait for actual dispatch, not an arbitrary sleep. TaskNum excludes the
  // blocked running task and includes only ready work, not pending timers.
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (executor->TaskNum() < 3 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  EXPECT_EQ(executor->TaskNum(), 3);
  for (auto& callback : callbacks) {
    EXPECT_EQ(callback.wait_for(std::chrono::milliseconds(0)),
              std::future_status::timeout);
  }
  release.set_value();
  for (auto& callback : callbacks) {
    const bool ready =
        callback.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
    EXPECT_TRUE(ready);
    if (ready && worker_entered) {
      EXPECT_EQ(callback.get(), worker_id);
    }
  }
  EXPECT_TRUE(executor->Stop());
}

TEST(ExecutorImplTest, PositiveDelayNeverRunsEarly) {
  ExecutorImpl executor("unit_test_exec", 1);
  ASSERT_TRUE(executor.Start());

  using Clock = std::chrono::steady_clock;
  auto completed = std::make_shared<std::promise<Clock::time_point>>();
  auto completion = completed->get_future();
  const auto before_schedule = Clock::now();
  constexpr int kDelayMs = 75;
  EXPECT_TRUE(executor.Schedule(
      [completed] { completed->set_value(Clock::now()); }, kDelayMs));
  const bool ready =
      completion.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
  EXPECT_TRUE(ready);
  if (ready) {
    EXPECT_GE(completion.get() - before_schedule,
              std::chrono::milliseconds(kDelayMs));
  }
  EXPECT_TRUE(executor.Stop());
}

TEST(ExecutorImplTest, StopDestroysPendingScheduledTask) {
  ExecutorImpl executor("unit_test_exec", 1);
  ASSERT_TRUE(executor.Start());

  auto ran = std::make_shared<std::atomic<bool>>(false);
  auto destroyed_on = std::make_shared<std::thread::id>();
  auto lifetime = std::shared_ptr<int>(new int(1), [destroyed_on](int* value) {
    *destroyed_on = std::this_thread::get_id();
    delete value;
  });
  std::weak_ptr<int> weak_lifetime = lifetime;
  ASSERT_TRUE(executor.Schedule(
      [lifetime = std::move(lifetime), ran] { ran->store(true); },
      60 * 60 * 1000));

  EXPECT_FALSE(weak_lifetime.expired());
  EXPECT_EQ(executor.TaskNum(), 0);
  EXPECT_TRUE(executor.Stop());
  EXPECT_FALSE(ran->load());
  EXPECT_TRUE(weak_lifetime.expired());
  // With the three-layer architecture, TimerImpl::Stop releases captures on
  // the EventBase timer thread (which is joined before Stop returns), not on
  // the Stop caller thread. The contract is that destruction happens before
  // Stop returns and the task never runs.
  EXPECT_NE(*destroyed_on, std::thread::id{});
}

TEST(ExecutorImplTest, ScheduleRacingStopReleasesAcceptedAndRejectedCaptures) {
  auto executor = std::make_shared<ExecutorImpl>("unit_test_exec", 1);
  ASSERT_TRUE(executor->Start());

  auto ran = std::make_shared<std::atomic<bool>>(false);
  struct RaceState {
    std::vector<std::weak_ptr<int>> lifetimes;
    int accepted = 0;
    int rejected = 0;
    int retained_rejections = 0;
  };
  auto state = std::make_shared<RaceState>();
  auto schedule = [executor, state, ran] {
    auto lifetime = std::make_shared<int>(1);
    std::weak_ptr<int> weak_lifetime = lifetime;
    const bool ok = executor->Schedule(
        [lifetime = std::move(lifetime), ran] { ran->store(true); },
        60 * 60 * 1000);
    state->lifetimes.push_back(weak_lifetime);
    if (ok) {
      ++state->accepted;
    } else {
      ++state->rejected;
      if (!weak_lifetime.expired()) {
        ++state->retained_rejections;
      }
    }
  };

  // Guarantee both admission outcomes independently of race scheduling.
  schedule();
  std::promise<void> begin;
  auto begin_future = begin.get_future().share();
  auto producer_done = std::make_shared<std::promise<void>>();
  auto producer_future = producer_done->get_future();
  std::thread producer([schedule, begin_future, producer_done] {
    begin_future.wait();
    for (int i = 0; i < 512; ++i) {
      schedule();
    }
    producer_done->set_value();
  });
  AsyncStop stop(executor, begin_future);
  begin.set_value();
  const bool producer_finished =
      producer_future.wait_for(std::chrono::seconds(5)) ==
      std::future_status::ready;
  if (producer_finished) {
    producer.join();
  } else {
    producer.detach();
  }
  ASSERT_TRUE(producer_finished) << "Schedule deadlocked while racing Stop";
  const bool stopped = stop.Finish();
  EXPECT_TRUE(stopped);
  if (stopped) {
    schedule();
  }
  EXPECT_GT(state->accepted, 0);
  EXPECT_GT(state->rejected, 0);
  EXPECT_EQ(state->retained_rejections, 0);
  EXPECT_FALSE(ran->load());
  for (const auto& lifetime : state->lifetimes) {
    EXPECT_TRUE(lifetime.expired());
  }
}

TEST(ExecutorImplTest, RestartDoesNotRunCancelledCallbacks) {
  ExecutorImpl executor("unit_test_exec", 1);
  auto old_ran = std::make_shared<std::atomic<bool>>(false);
  auto old_lifetime = std::make_shared<int>(1);
  std::weak_ptr<int> weak_old_lifetime = old_lifetime;
  ASSERT_TRUE(executor.Start());
  ASSERT_TRUE(executor.Schedule([old_lifetime = std::move(old_lifetime),
                                 old_ran] { old_ran->store(true); },
                                60 * 60 * 1000));
  ASSERT_TRUE(executor.Stop());
  EXPECT_TRUE(weak_old_lifetime.expired());

  ASSERT_TRUE(executor.Start());
  auto completed = std::make_shared<std::promise<void>>();
  auto completion = completed->get_future();
  EXPECT_TRUE(executor.Schedule([completed] { completed->set_value(); }, 10));
  EXPECT_EQ(completion.wait_for(std::chrono::seconds(5)),
            std::future_status::ready);
  EXPECT_FALSE(old_ran->load());
  EXPECT_TRUE(executor.Stop());
}

TEST(ExecutorImplTest, CancelledCaptureDestructorCanReenterExecutor) {
  auto executor = std::make_shared<ExecutorImpl>("unit_test_exec", 1);
  ASSERT_TRUE(executor->Start());

  struct Result {
    std::atomic<bool> destroyed{false};
    std::atomic<bool> rejected{false};
    std::atomic<bool> rejected_capture_released{false};
    std::atomic<int> queued{-1};
  };
  auto result = std::make_shared<Result>();
  std::weak_ptr<ExecutorImpl> weak_executor = executor;
  auto lifetime =
      std::shared_ptr<int>(new int(1), [weak_executor, result](int* value) {
        delete value;
        auto executor = weak_executor.lock();
        auto nested = std::make_shared<int>(1);
        std::weak_ptr<int> weak_nested = nested;
        result->rejected = !executor->Schedule([nested = std::move(nested)] {},
                                               60 * 60 * 1000);
        result->rejected_capture_released = weak_nested.expired();
        result->queued = executor->TaskNum();
        result->destroyed = true;
      });
  std::weak_ptr<int> weak_lifetime = lifetime;
  EXPECT_TRUE(
      executor->Schedule([lifetime = std::move(lifetime)] {}, 60 * 60 * 1000));
  AsyncStop stop(executor);
  ASSERT_TRUE(stop.Finish()) << "Stop deadlocked during capture destruction";
  EXPECT_TRUE(weak_lifetime.expired());
  EXPECT_TRUE(result->destroyed.load());
  EXPECT_TRUE(result->rejected.load());
  EXPECT_TRUE(result->rejected_capture_released.load());
  EXPECT_EQ(result->queued.load(), 0);
}

}  // namespace unit_test
}  // namespace dingofs
