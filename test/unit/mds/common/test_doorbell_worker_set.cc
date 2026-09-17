// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "mds/common/runnable.h"
#include "utils/time.h"

namespace dingofs {
namespace mds {
namespace unit_test {

static bool WaitUntil(const std::function<bool()>& done,
                      int64_t timeout_ms = 10000) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
  while (!done()) {
    if (std::chrono::steady_clock::now() > deadline) return false;
    std::this_thread::yield();
  }
  return true;
}

// Counts its own execution. `run_count` is per task, so a double run is
// visible.
class CountTask : public TaskRunnable {
 public:
  CountTask(std::atomic<int64_t>* total, std::atomic<int64_t>* run_count,
            const std::atomic<bool>* gate = nullptr)
      : total_(total), run_count_(run_count), gate_(gate) {}

  std::string Type() override { return "COUNT"; }

  void Run() override {
    // Held shut by the test until it is about to call Stop(), so a queue that
    // is non-empty at Stop() is guaranteed rather than hoped for.
    while (gate_ != nullptr && !gate_->load(std::memory_order_acquire))
      std::this_thread::yield();

    run_count_->fetch_add(1, std::memory_order_relaxed);
    total_->fetch_add(1, std::memory_order_relaxed);
  }

 private:
  std::atomic<int64_t>* total_;
  std::atomic<int64_t>* run_count_;
  const std::atomic<bool>* gate_;
};

// Bumps one counter and gets out of the way: the perf test measures the submit
// path, so the tasks themselves must stay trivial.
class NopTask : public TaskRunnable {
 public:
  explicit NopTask(std::atomic<int64_t>* total) : total_(total) {}

  std::string Type() override { return "NOP"; }

  void Run() override { total_->fetch_add(1, std::memory_order_relaxed); }

 private:
  std::atomic<int64_t>* total_;
};

// Records where it ran in its hash group's sequence.
class OrderTask : public TaskRunnable {
 public:
  OrderTask(std::atomic<uint64_t>* cursor, uint64_t expected,
            std::atomic<int64_t>* violations)
      : cursor_(cursor), expected_(expected), violations_(violations) {}

  std::string Type() override { return "ORDER"; }
  std::string Key() override { return std::to_string(expected_); }

  void Run() override {
    if (cursor_->fetch_add(1, std::memory_order_relaxed) != expected_) {
      violations_->fetch_add(1, std::memory_order_relaxed);
    }
  }

 private:
  std::atomic<uint64_t>* cursor_;
  const uint64_t expected_;
  std::atomic<int64_t>* violations_;
};

// Tasks hashed to one worker must run in submission order: that is the whole
// reason AsyncOpen/AsyncClose use the ino as the hash.
TEST(DoorbellWorkerSetTest, HashKeepsSubmissionOrder) {
  constexpr uint32_t kWorkerNum = 4;
  constexpr uint32_t kInoNum = 4;
  constexpr uint64_t kTasksPerIno = 2000;

  DoorbellWorkerSet worker_set("fast_order", kWorkerNum, 0);
  ASSERT_TRUE(worker_set.Init());

  std::array<std::atomic<uint64_t>, kInoNum> cursors{};
  std::atomic<int64_t> violations{0};

  for (uint64_t i = 0; i < kTasksPerIno; ++i) {
    for (uint32_t ino = 0; ino < kInoNum; ++ino) {
      ASSERT_TRUE(worker_set.ExecuteHash(
          ino, std::make_shared<OrderTask>(&cursors[ino], i, &violations)));
    }
  }

  EXPECT_TRUE(WaitUntil([&] {
    for (uint32_t ino = 0; ino < kInoNum; ++ino) {
      if (cursors[ino].load(std::memory_order_relaxed) != kTasksPerIno)
        return false;
    }
    return true;
  }));

  EXPECT_EQ(violations.load(std::memory_order_relaxed), 0);

  worker_set.Stop();
}

// Every submission goes through all four entry points and must run exactly
// once.
TEST(DoorbellWorkerSetTest, EveryTaskRunsExactlyOnce) {
  constexpr uint32_t kWorkerNum = 8;
  constexpr int64_t kTaskNum = 20000;
  constexpr int64_t kProducerNum = 8;

  DoorbellWorkerSet worker_set("fast_once", kWorkerNum, 0);
  ASSERT_TRUE(worker_set.Init());

  std::unique_ptr<std::atomic<int64_t>[]> run_counts(
      new std::atomic<int64_t>[kTaskNum]);
  for (int64_t i = 0; i < kTaskNum; ++i)
    run_counts[i].store(0, std::memory_order_relaxed);

  std::atomic<int64_t> total{0};

  std::vector<std::thread> producers;
  for (int64_t p = 0; p < kProducerNum; ++p) {
    producers.emplace_back([&worker_set, &total, &run_counts, p] {
      for (int64_t i = p; i < kTaskNum; i += kProducerNum) {
        auto task = std::make_shared<CountTask>(&total, &run_counts[i]);
        switch (i % 3) {
          case 0:
            EXPECT_TRUE(worker_set.ExecuteHash(
                static_cast<uint64_t>(i) * 0x9e3779b97f4a7c15ull, task));
            break;
          case 1:
            EXPECT_TRUE(worker_set.ExecuteRR(task));
            break;
          default:
            EXPECT_TRUE(worker_set.ExecuteLeastQueue(task));
            break;
        }
      }
    });
  }
  for (auto& producer : producers) producer.join();

  ASSERT_TRUE(WaitUntil(
      [&] { return total.load(std::memory_order_relaxed) == kTaskNum; }));

  for (int64_t i = 0; i < kTaskNum; ++i) {
    EXPECT_EQ(run_counts[i].load(std::memory_order_relaxed), 1) << "task " << i;
  }
  EXPECT_EQ(worker_set.PendingTaskCount(), 0);

  worker_set.Stop();
}

// Stop() must run everything already queued, not drop it: those tasks carry the
// MDS calls that register and unregister file sessions.
TEST(DoorbellWorkerSetTest, StopDrainsQueuedTasks) {
  constexpr int64_t kTaskNum = 200;

  // One worker: the gate freezes the task it is running, so every other task is
  // still in the queue when Stop() is called.
  DoorbellWorkerSet worker_set("fast_drain", 1, 0);
  ASSERT_TRUE(worker_set.Init());

  std::atomic<bool> gate{false};
  std::unique_ptr<std::atomic<int64_t>[]> run_counts(
      new std::atomic<int64_t>[kTaskNum]);
  for (int64_t i = 0; i < kTaskNum; ++i)
    run_counts[i].store(0, std::memory_order_relaxed);

  std::atomic<int64_t> total{0};
  for (int64_t i = 0; i < kTaskNum; ++i) {
    ASSERT_TRUE(worker_set.ExecuteHash(
        i, std::make_shared<CountTask>(&total, &run_counts[i], &gate)));
  }

  // Nothing can have completed while the gate is shut.
  EXPECT_EQ(worker_set.PendingTaskCount(), kTaskNum);

  gate.store(true, std::memory_order_release);
  worker_set.Stop();

  EXPECT_EQ(worker_set.PendingTaskCount(), 0);
  EXPECT_EQ(total.load(std::memory_order_relaxed), kTaskNum);
  for (int64_t i = 0; i < kTaskNum; ++i) {
    EXPECT_EQ(run_counts[i].load(std::memory_order_relaxed), 1) << "task " << i;
  }

  worker_set.Stop();  // idempotent
}

// The submit/stop race: whatever ExecuteHash() accepted must run, and nothing
// it rejected may run. A consumer that leaves with an accepted task still in
// flight loses it here.
TEST(DoorbellWorkerSetTest, StopDoesNotDropTasksSubmittedConcurrently) {
  constexpr uint32_t kWorkerNum = 4;
  constexpr int64_t kProducerNum = 4;
  constexpr int64_t kPerProducer = 20000;
  constexpr int64_t kTaskNum = kProducerNum * kPerProducer;

  DoorbellWorkerSet worker_set("fast_race", kWorkerNum, 0);
  ASSERT_TRUE(worker_set.Init());

  std::unique_ptr<std::atomic<int64_t>[]> run_counts(
      new std::atomic<int64_t>[kTaskNum]);
  std::unique_ptr<std::atomic<bool>[]> accepted(
      new std::atomic<bool>[kTaskNum]);
  for (int64_t i = 0; i < kTaskNum; ++i) {
    run_counts[i].store(0, std::memory_order_relaxed);
    accepted[i].store(false, std::memory_order_relaxed);
  }

  std::atomic<int64_t> total{0};
  std::atomic<int64_t> accepted_count{0};

  std::vector<std::thread> producers;
  for (int64_t p = 0; p < kProducerNum; ++p) {
    producers.emplace_back([&, p] {
      for (int64_t i = 0; i < kPerProducer; ++i) {
        const int64_t slot = p * kPerProducer + i;
        if (!worker_set.ExecuteHash(
                slot, std::make_shared<CountTask>(&total, &run_counts[slot]))) {
          break;  // rejected: the worker set is stopping
        }
        accepted[slot].store(true, std::memory_order_relaxed);
        accepted_count.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(2));
  worker_set.Stop();

  for (auto& producer : producers) producer.join();

  ASSERT_GT(accepted_count.load(std::memory_order_relaxed), 0);
  EXPECT_EQ(worker_set.PendingTaskCount(), 0);

  int64_t expected = 0;
  for (int64_t i = 0; i < kTaskNum; ++i) {
    const int64_t want = accepted[i].load(std::memory_order_relaxed) ? 1 : 0;
    EXPECT_EQ(run_counts[i].load(std::memory_order_relaxed), want)
        << "task " << i;
    expected += want;
  }
  EXPECT_EQ(total.load(std::memory_order_relaxed), expected);
}

// Reports the cost of the submit path on its own -- tasks are built before the
// clock starts, so no allocation lands inside the measurement -- and then the
// cost of draining them. Skipped unless explicitly requested.
TEST(DoorbellWorkerSetTest, ExecuteHashPerformance) {
  if (getenv("MANUAL_TEST") == nullptr) {
    GTEST_SKIP() << "Skip manual test case.";
  }

  constexpr uint32_t kWorkerNum = 64;
  constexpr int64_t kProducerNum = 8;
  constexpr int64_t kPerProducer = 125000;
  constexpr int64_t kTaskNum = kProducerNum * kPerProducer;

  DoorbellWorkerSet worker_set("fast_perf", kWorkerNum, 0);
  ASSERT_TRUE(worker_set.Init());

  std::atomic<int64_t> total{0};
  std::atomic<int64_t> rejected{0};
  std::atomic<bool> start{false};

  std::vector<std::vector<TaskRunnablePtr>> tasks(kProducerNum);
  for (int64_t p = 0; p < kProducerNum; ++p) {
    tasks[p].reserve(kPerProducer);
    for (int64_t i = 0; i < kPerProducer; ++i) {
      tasks[p].push_back(std::make_shared<NopTask>(&total));
    }
  }

  std::vector<std::thread> producers;
  producers.reserve(kProducerNum);
  for (int64_t p = 0; p < kProducerNum; ++p) {
    producers.emplace_back([&, p] {
      while (!start.load(std::memory_order_acquire)) std::this_thread::yield();

      for (int64_t i = 0; i < kPerProducer; ++i) {
        // Spread ids over the workers, as the meta executor does with inos.
        const uint64_t id = static_cast<uint64_t>(p * kPerProducer + i);
        if (!worker_set.ExecuteHash(id, tasks[p][i])) {
          rejected.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }

  utils::Duration duration;
  start.store(true, std::memory_order_release);
  for (auto& producer : producers) producer.join();
  const int64_t submit_us = duration.ElapsedUs();

  ASSERT_TRUE(WaitUntil(
      [&] { return total.load(std::memory_order_relaxed) == kTaskNum; },
      60000));
  const int64_t all_us = duration.ElapsedUs();

  std::cout << "DoorbellWorkerSet ExecuteHash: workers=" << kWorkerNum
            << " producers=" << kProducerNum << " tasks=" << kTaskNum << "\n"
            << "  submit: " << submit_us << " us, "
            << (kTaskNum * 1000000.0 / submit_us) << " tasks/s, "
            << (submit_us * 1000.0 / kTaskNum) << " ns/task\n"
            << "  submit+run: " << all_us << " us, "
            << (kTaskNum * 1000000.0 / all_us) << " tasks/s\n";

  EXPECT_EQ(rejected.load(std::memory_order_relaxed), 0);
  EXPECT_EQ(worker_set.PendingTaskCount(), 0);

  worker_set.Stop();
}

TEST(DoorbellWorkerSetTest, ExecuteAfterStopIsRejected) {
  DoorbellWorkerSet worker_set("fast_rejected", 2, 0);
  ASSERT_TRUE(worker_set.Init());

  std::atomic<int64_t> total{0};
  std::atomic<int64_t> run_count{0};

  ASSERT_TRUE(worker_set.ExecuteHash(
      1, std::make_shared<CountTask>(&total, &run_count)));
  worker_set.Stop();

  EXPECT_TRUE(worker_set.IsStopped());
  EXPECT_FALSE(worker_set.ExecuteHash(
      1, std::make_shared<CountTask>(&total, &run_count)));
  EXPECT_FALSE(
      worker_set.ExecuteRR(std::make_shared<CountTask>(&total, &run_count)));
  EXPECT_FALSE(worker_set.ExecuteLeastQueue(
      std::make_shared<CountTask>(&total, &run_count)));
  EXPECT_FALSE(worker_set.Execute(nullptr));

  EXPECT_EQ(total.load(std::memory_order_relaxed), 1);
  EXPECT_EQ(run_count.load(std::memory_order_relaxed), 1);
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
