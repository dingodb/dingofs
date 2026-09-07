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

#include "blockcache/core/runtime/worker_pool.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <semaphore>
#include <thread>
#include <vector>

#include "blockcache/core/reactor/doorbell.h"

namespace dingofs {
namespace blockcache {
namespace {

class CyclicBarrier {
 public:
  explicit CyclicBarrier(unsigned parties) : parties_(parties) {}

  void Wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    const unsigned generation = generation_;
    if (++arrived_ == parties_) {
      arrived_ = 0;
      ++generation_;
      cv_.notify_all();
      return;
    }
    cv_.wait(lock, [this, generation] { return generation_ != generation; });
  }

 private:
  const unsigned parties_;
  unsigned arrived_ = 0;
  unsigned generation_ = 0;
  std::mutex mutex_;
  std::condition_variable cv_;
};

TEST(DoorbellTest, ExactlyOneConcurrentWakerClaimsEachArm) {
  constexpr unsigned kWakers = 8;
  constexpr unsigned kRounds = 5000;

  Doorbell bell;
  CyclicBarrier start(kWakers + 1);
  CyclicBarrier finish(kWakers + 1);
  std::atomic<unsigned> claims{0};
  std::vector<std::thread> wakers;
  wakers.reserve(kWakers);

  for (unsigned i = 0; i < kWakers; ++i) {
    wakers.emplace_back([&] {
      for (unsigned round = 0; round < kRounds; ++round) {
        start.Wait();
        if (bell.ClaimWakeup()) {
          claims.fetch_add(1, std::memory_order_relaxed);
        }
        finish.Wait();
      }
    });
  }

  unsigned bad_rounds = 0;
  for (unsigned round = 0; round < kRounds; ++round) {
    claims.store(0, std::memory_order_relaxed);
    bell.Arm();
    start.Wait();
    finish.Wait();
    if (claims.load(std::memory_order_relaxed) != 1) {
      ++bad_rounds;
    }
  }

  for (auto& waker : wakers) {
    waker.join();
  }
  EXPECT_EQ(bad_rounds, 0);
  EXPECT_FALSE(bell.ClaimWakeup());
}

TEST(ParkerTest, WakeBetweenArmAndParkIsNotLost) {
  Parker parker;
  std::binary_semaphore armed{0};
  std::binary_semaphore woke{0};
  std::atomic<int> ready_calls{0};

  std::thread waker([&] {
    armed.acquire();
    parker.Wake();
    woke.release();
  });

  // The first ready() call runs between Arm() and the lock: waking exactly
  // there is the window where a bare notify evaporates.
  const bool woken = parker.WaitFor(
      [&] {
        if (ready_calls.fetch_add(1, std::memory_order_relaxed) == 0) {
          armed.release();
          woke.acquire();
        }
        return false;
      },
      200'000'000);

  waker.join();
  EXPECT_TRUE(woken);
}

struct CompletionState {
  std::atomic<size_t> completed{0};
  std::mutex mutex;
  std::condition_variable cv;
};

struct TestWork : InboxWork {
  TestWork(CompletionState* state, std::atomic<uint8_t>* execution_count)
      : state(state), execution_count(execution_count) {
    run = [](InboxWork* base) {
      std::unique_ptr<TestWork> self(static_cast<TestWork*>(base));
      self->execution_count->fetch_add(1, std::memory_order_relaxed);
      {
        std::lock_guard<std::mutex> lock(self->state->mutex);
        self->state->completed.fetch_add(1, std::memory_order_release);
      }
      self->state->cv.notify_one();
    };
  }

  CompletionState* state;
  std::atomic<uint8_t>* execution_count;
};

TEST(CpuWorkerTest, RepeatedParkWakeWithConcurrentProducersRunsExactlyOnce) {
  constexpr unsigned kProducers = 8;
  constexpr unsigned kRounds = 1000;
  constexpr auto kRoundTimeout = std::chrono::seconds(2);

  CpuWorker worker;
  worker.Start(0);

  CompletionState completion;
  std::vector<std::atomic<uint8_t>> execution_counts(kProducers * kRounds);
  for (auto& count : execution_counts) {
    count.store(0, std::memory_order_relaxed);
  }

  std::mutex producer_mutex;
  std::condition_variable producer_cv;
  unsigned generation = 0;
  unsigned posted = 0;
  bool stop = false;
  std::vector<std::thread> producers;
  producers.reserve(kProducers);

  for (unsigned producer = 0; producer < kProducers; ++producer) {
    producers.emplace_back([&, producer] {
      unsigned local_generation = 0;
      for (;;) {
        std::unique_lock<std::mutex> lock(producer_mutex);
        producer_cv.wait(
            lock, [&] { return stop || generation != local_generation; });
        if (stop) {
          return;
        }
        local_generation = generation;
        const size_t id = ((local_generation - 1) * kProducers) + producer;
        lock.unlock();

        worker.Post(new TestWork(&completion, &execution_counts[id]));

        lock.lock();
        ++posted;
        producer_cv.notify_all();
      }
    });
  }

  bool timed_out = false;
  for (unsigned round = 1; round <= kRounds; ++round) {
    {
      std::lock_guard<std::mutex> lock(producer_mutex);
      posted = 0;
      generation = round;
    }
    producer_cv.notify_all();

    {
      std::unique_lock<std::mutex> lock(producer_mutex);
      producer_cv.wait(lock, [&] { return posted == kProducers; });
    }

    const size_t target = round * kProducers;
    std::unique_lock<std::mutex> lock(completion.mutex);
    if (!completion.cv.wait_for(lock, kRoundTimeout, [&] {
          return completion.completed.load(std::memory_order_acquire) >= target;
        })) {
      timed_out = true;
      break;
    }

    // Give the consumer enough idle time to exercise its park path each round.
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }

  {
    std::lock_guard<std::mutex> lock(producer_mutex);
    stop = true;
  }
  producer_cv.notify_all();
  for (auto& producer : producers) {
    producer.join();
  }
  worker.Shutdown();

  ASSERT_FALSE(timed_out);
  EXPECT_EQ(completion.completed.load(std::memory_order_acquire),
            execution_counts.size());
  for (const auto& count : execution_counts) {
    EXPECT_EQ(count.load(std::memory_order_relaxed), 1);
  }
}

}  // namespace
}  // namespace blockcache
}  // namespace dingofs
