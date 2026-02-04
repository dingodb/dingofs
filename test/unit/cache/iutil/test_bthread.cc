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
 * Author: AI
 */

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "cache/helper/test_log_sink.h"
#include "cache/iutil/bthread.h"

namespace dingofs {
namespace cache {
namespace iutil {

using test::TestLogSink;

TEST(BthreadTest, RunInBthread) {
  std::atomic<bool> executed{false};
  std::atomic<bthread_t> executed_tid{0};

  bthread_t tid = RunInBthread([&]() {
    executed = true;
    executed_tid = bthread_self();
  });

  EXPECT_NE(tid, 0);
  bthread_join(tid, nullptr);

  EXPECT_TRUE(executed.load());
  EXPECT_EQ(executed_tid.load(), tid);
}

TEST(BthreadTest, RunMultipleBthreads) {
  const int kThreadCount = 10;
  std::atomic<int> counter{0};
  std::vector<bthread_t> tids;

  for (int i = 0; i < kThreadCount; i++) {
    bthread_t tid = RunInBthread([&]() { counter.fetch_add(1); });
    EXPECT_NE(tid, 0);
    tids.push_back(tid);
  }

  for (auto tid : tids) {
    bthread_join(tid, nullptr);
  }

  EXPECT_EQ(counter.load(), kThreadCount);
}

TEST(BthreadJoinerTest, StartAndShutdown) {
  const std::string kStartLog = "BthreadJoiner is starting...";
  const std::string kShutdownLog = "BthreadJoiner is shutting down...";

  TestLogSink log_sink;
  log_sink.Register(kStartLog);
  log_sink.Register(kShutdownLog);
  google::AddLogSink(&log_sink);

  {
    log_sink.Reset();
    BthreadJoiner joiner;
    joiner.Start();
    joiner.Start();
    joiner.Shutdown();
    joiner.Shutdown();
    EXPECT_EQ(log_sink.Count(kStartLog), 1);
    EXPECT_EQ(log_sink.Count(kShutdownLog), 1);
  }

  {
    log_sink.Reset();
    BthreadJoiner joiner;
    joiner.Shutdown();
    EXPECT_EQ(log_sink.Count(kStartLog), 0);
    EXPECT_EQ(log_sink.Count(kShutdownLog), 0);
  }

  {
    log_sink.Reset();
    BthreadJoiner joiner;
    joiner.Start();
    joiner.Shutdown();
    joiner.Start();
    joiner.Shutdown();
    EXPECT_EQ(log_sink.Count(kStartLog), 2);
    EXPECT_EQ(log_sink.Count(kShutdownLog), 2);
  }

  google::RemoveLogSink(&log_sink);
}

TEST(BthreadJoinerTest, BackgroundJoin) {
  BthreadJoiner joiner;
  joiner.Start();

  std::atomic<bool> executed{false};

  bthread_t tid = RunInBthread([&]() {
    executed = true;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  });

  EXPECT_NE(tid, 0);
  EXPECT_EQ(bthread_stopped(tid), 0);

  joiner.BackgroundJoin(tid);
  joiner.Shutdown();

  EXPECT_TRUE(executed.load());
  EXPECT_EQ(bthread_stopped(tid), 1);
}

TEST(BthreadJoinerTest, BackgroundJoinMultipleBthreads) {
  BthreadJoiner joiner;
  joiner.Start();

  const int kThreadCount = 20;
  std::atomic<int> counter{0};
  std::vector<bthread_t> tids;

  for (int i = 0; i < kThreadCount; i++) {
    bthread_t tid = RunInBthread([&]() {
      counter.fetch_add(1);
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    });
    EXPECT_NE(tid, 0);
    tids.push_back(tid);
    joiner.BackgroundJoin(tid);
  }

  joiner.Shutdown();

  EXPECT_EQ(counter.load(), kThreadCount);
  for (auto tid : tids) {
    EXPECT_EQ(bthread_stopped(tid), 1);
  }
}

TEST(BthreadJoinerTest, BackgroundJoinConcurrent) {
  BthreadJoiner joiner;
  joiner.Start();

  const int kThreadCount = 50;
  std::atomic<int> counter{0};
  std::vector<bthread_t> all_tids;
  std::mutex tids_mutex;
  std::vector<std::thread> producers;
  producers.reserve(5);

  for (int i = 0; i < 5; i++) {
    producers.emplace_back([&]() {
      for (int j = 0; j < kThreadCount / 5; j++) {
        bthread_t tid = RunInBthread([&]() { counter.fetch_add(1); });
        {
          std::lock_guard<std::mutex> lock(tids_mutex);
          all_tids.push_back(tid);
        }
        joiner.BackgroundJoin(tid);
      }
    });
  }

  for (auto& t : producers) {
    t.join();
  }

  joiner.Shutdown();

  EXPECT_EQ(counter.load(), kThreadCount);
  for (auto tid : all_tids) {
    EXPECT_EQ(bthread_stopped(tid), 1);
  }
}

TEST(BthreadJoinerTest, DestructorCallsShutdown) {
  std::atomic<bool> executed{false};
  bthread_t saved_tid = 0;

  {
    BthreadJoiner joiner;
    joiner.Start();

    saved_tid = RunInBthread([&]() {
      executed = true;
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    });

    joiner.BackgroundJoin(saved_tid);
  }

  EXPECT_TRUE(executed.load());
  EXPECT_EQ(bthread_stopped(saved_tid), 1);
}

}  // namespace iutil
}  // namespace cache
}  // namespace dingofs
