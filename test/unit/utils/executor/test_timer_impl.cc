// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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

#include <unistd.h>

#include <atomic>
#include <chrono>              // NOLINT
#include <condition_variable>  // NOLINT
#include <future>
#include <memory>
#include <mutex>   // NOLINT
#include <thread>  // NOLINT

#include "common/sync_point.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "utils/executor/thread/thread_pool_impl.h"
#include "utils/executor/timer/timer_impl.h"
#include "utils/scoped_cleanup.h"

namespace dingofs {
namespace utils {
namespace unit_test {

class TimerImplTest : public ::testing::Test {
 public:
  TimerImplTest() {
    pool = std::make_unique<ThreadPoolImpl>("unit_test", 2);
    pool->Start();
  }

  ~TimerImplTest() override { pool->Stop(); }

  std::unique_ptr<ThreadPoolImpl> pool{nullptr};
};

TEST_F(TimerImplTest, BaseTest) {
  auto timer = std::make_unique<TimerImpl>(pool.get());
  EXPECT_TRUE(timer->Start());

  EXPECT_TRUE(timer->Stop());

  EXPECT_FALSE(timer->Stop());
}

TEST_F(TimerImplTest, Add) {
  auto timer = std::make_unique<TimerImpl>(pool.get());

  EXPECT_TRUE(timer->Start());

  std::mutex mutex;
  std::condition_variable cond;
  std::atomic<int> count(2);

  timer->Add(
      [&]() {
        EXPECT_EQ(count.fetch_sub(1), 2);
        cond.notify_all();
      },
      5);

  timer->Add(
      [&]() {
        EXPECT_EQ(count.fetch_sub(1), 1);
        cond.notify_all();
      },
      10);

  {
    std::unique_lock<std::mutex> lg(mutex);
    while (count.load() != 0) {
      LOG(INFO) << "wait 1 ms";
      cond.wait_for(lg, std::chrono::milliseconds(1));
    }
  }

  EXPECT_EQ(count.load(), 0);
  timer->Stop();
}

TEST_F(TimerImplTest, EarlierDeadlineInterruptsTimedWait) {
#ifdef NDEBUG
  GTEST_SKIP() << "Deterministic timer waiting requires TEST_SYNC_POINT; "
                  "run this regression in a Debug build.";
#else
  std::promise<void> waiting;
  auto waiting_future = waiting.get_future();
  std::once_flag waiting_once;
  std::promise<void> earlier_ran;
  auto earlier_future = earlier_ran.get_future();
  std::atomic<bool> later_ran{false};
  auto timer = std::make_unique<TimerImpl>(pool.get());

  auto cleanup = MakeScopedCleanup([&] {
    timer->Stop();
    pool->Stop();
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  SyncPoint::GetInstance()->SetCallBack(
      "TimerImpl::Run:before_timed_wait", [&](void* arg) {
        if (arg == timer.get()) {
          std::call_once(waiting_once, [&] { waiting.set_value(); });
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_TRUE(timer->Start());
  ASSERT_TRUE(timer->Add([&] { later_ran.store(true); }, 60 * 60 * 1000));
  ASSERT_EQ(waiting_future.wait_for(std::chrono::seconds(5)),
            std::future_status::ready)
      << "Timer did not reach the wait for the distant deadline";

  // The sync point runs with the timer mutex held. Add cannot acquire it until
  // wait_for atomically registers its waiter and releases that mutex.
  ASSERT_TRUE(timer->Add([&] { earlier_ran.set_value(); }, 10));
  EXPECT_EQ(earlier_future.wait_for(std::chrono::seconds(5)),
            std::future_status::ready)
      << "The earlier deadline did not interrupt the existing timed wait";
  EXPECT_FALSE(later_ran.load());
#endif
}

TEST_F(TimerImplTest, StopDestroysPendingFunctionsOutsideMutex) {
  auto timer = std::make_unique<TimerImpl>(pool.get());
  ASSERT_TRUE(timer->Start());

  std::atomic<bool> ran{false};
  std::atomic<bool> destroyed{false};
  auto probe = std::make_shared<int>(1);
  std::weak_ptr<int> weak_probe = probe;
  ASSERT_TRUE(timer->Add([probe, &ran] { ran.store(true); }, 60 * 60 * 1000));
  probe.reset();

  ASSERT_FALSE(weak_probe.expired());
  ASSERT_TRUE(timer->Stop());
  EXPECT_FALSE(ran.load());
  EXPECT_TRUE(weak_probe.expired());
}

}  // namespace unit_test
}  // namespace utils
}  // namespace dingofs
