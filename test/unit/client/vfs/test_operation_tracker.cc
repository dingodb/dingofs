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

#include <gtest/gtest.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <future>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

#include "client/vfs/operation_tracker.h"

namespace dingofs {
namespace client {

namespace {

using Tracker = OperationTracker;
constexpr auto kCheckpointTimeout = std::chrono::seconds(5);

// A missing wakeup cannot be repaired safely by destroying a tracker or
// detaching its callers. Success joins every thread; a synchronization failure
// terminates the entire child. The watchdog is shorter than the production
// diagnostic wakeup, so it cannot hide a missed notification when a wait
// occurs. Checkpoints below coordinate public calls, not internal CV wait
// entry.
void RunBounded(void (*scenario)()) {
  // Other suites may already have background threads; re-exec instead of
  // inheriting their potentially locked mutexes into a fork-only child.
  const auto previous_style = ::testing::FLAGS_gtest_death_test_style;
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  EXPECT_EXIT(
      {
        alarm(20);
        scenario();
        if (::testing::Test::HasFailure()) {
          std::fprintf(stderr, "Tracker scenario assertion failed\n");
          _exit(1);
        }
        _exit(0);
      },
      ::testing::ExitedWithCode(0), "");
  ::testing::FLAGS_gtest_death_test_style = previous_style;
}

class Checkpoint {
 public:
  void Hit() {
    std::lock_guard<std::mutex> lock(mutex_);
    ++hits_;
    cv_.notify_all();
  }

  void Wait(unsigned count = 1) {
    std::unique_lock<std::mutex> lock(mutex_);
    CHECK(cv_.wait_for(lock, kCheckpointTimeout,
                       [this, count] { return hits_ >= count; }))
        << "Tracker checkpoint not reached: expected " << count << ", got "
        << hits_;
  }

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  unsigned hits_{0};
};

// Observe a lease-protected tail before joining the returning caller. Relaxed
// atomics keep a premature drain observable without making a failing test race
// on non-atomic memory. The tracker, not a join or a test checkpoint, must
// publish the tail to the draining thread.
void ReturnLeaseAndDrain(Tracker& tracker,
                         std::optional<Tracker::Lease> lease) {
  std::atomic<bool> body_finished{false};
  Checkpoint release;
  std::thread returner([&, lease = std::move(lease)]() mutable {
    release.Wait();
    body_finished.store(true, std::memory_order_relaxed);
    lease.reset();
  });
  release.Hit();
  tracker.WaitForDrain();
  EXPECT_TRUE(body_finished.load(std::memory_order_relaxed));
  returner.join();
}

void RunSlotContention() {
  Tracker tracker;
  tracker.OpenOnce();
  // More live acquiring OS threads than slots guarantees natural collisions,
  // without choosing a private slot or assuming a particular thread index.
  constexpr unsigned kWorkers = 2 * Tracker::kSlotCount;
  std::array<std::atomic<unsigned>, kWorkers> payload;
  for (auto& value : payload) value.store(0, std::memory_order_relaxed);
  Checkpoint admitted;
  Checkpoint release;
  std::vector<std::thread> workers;
  workers.reserve(kWorkers);
  for (unsigned i = 0; i < kWorkers; ++i) {
    workers.emplace_back([&, i] {
      auto lease = tracker.TryEnter();
      CHECK(lease.has_value());
      admitted.Hit();
      release.Wait();
      payload[i].store(i + 1, std::memory_order_relaxed);
      lease.reset();
    });
  }
  admitted.Wait(kWorkers);
  tracker.Close();
  release.Hit();
  tracker.WaitForDrain();
  for (unsigned i = 0; i < kWorkers; ++i) {
    EXPECT_EQ(payload[i].load(std::memory_order_relaxed), i + 1);
  }
  // WaitForDrain protects the payloads, not the tracker object's lifetime.
  for (auto& worker : workers) worker.join();
}

TEST(OperationTrackerTest, G01RejectsBeforeOpen) {
  Tracker tracker;
  unsigned body_calls = 0;
  if (auto lease = tracker.TryEnter()) ++body_calls;
  EXPECT_EQ(body_calls, 0);
  EXPECT_FALSE(tracker.TryEnter().has_value());
}

TEST(OperationTrackerTest, G02OpenPublishesInitialization) {
  RunBounded([] {
    Tracker tracker;
    std::array<uint64_t, 4> payload{};
    Checkpoint reader_ready;
    std::thread reader([&] {
      reader_ready.Hit();
      const auto deadline =
          std::chrono::steady_clock::now() + kCheckpointTimeout;
      for (;;) {
        if (auto lease = tracker.TryEnter()) {
          EXPECT_EQ(payload[0], 11);
          EXPECT_EQ(payload[1], 22);
          EXPECT_EQ(payload[2], 33);
          EXPECT_EQ(payload[3], 44);
          break;
        }
        CHECK(std::chrono::steady_clock::now() < deadline);
        std::this_thread::yield();
      }
    });
    reader_ready.Wait();
    // No test synchronization publishes these writes to the reader. Only the
    // OpenOnce epoch publication can make them visible before the body runs.
    payload = {11, 22, 33, 44};
    tracker.OpenOnce();
    reader.join();
    tracker.Close();
    tracker.WaitForDrain();
  });
}

TEST(OperationTrackerDeathTest, G03RepeatedOpenFails) {
  const auto previous_style = ::testing::FLAGS_gtest_death_test_style;
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  EXPECT_DEATH(
      {
        Tracker tracker;
        tracker.OpenOnce();
        tracker.OpenOnce();
      },
      "");
  ::testing::FLAGS_gtest_death_test_style = previous_style;
}

TEST(OperationTrackerDeathTest, G03ReopenAfterCloseFails) {
  const auto previous_style = ::testing::FLAGS_gtest_death_test_style;
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  EXPECT_DEATH(
      {
        Tracker tracker;
        tracker.OpenOnce();
        tracker.Close();
        tracker.OpenOnce();
      },
      "");
  ::testing::FLAGS_gtest_death_test_style = previous_style;
}

TEST(OperationTrackerTest, G04RejectsAllEntrantsAfterClose) {
  RunBounded([] {
    Tracker tracker;
    tracker.OpenOnce();
    tracker.Close();
    std::atomic<unsigned> body_calls{0};
    std::vector<std::thread> entrants;
    for (unsigned i = 0; i < 16; ++i) {
      entrants.emplace_back([&] {
        for (unsigned attempt = 0; attempt < 64; ++attempt) {
          if (auto lease = tracker.TryEnter()) ++body_calls;
        }
      });
    }
    for (auto& entrant : entrants) entrant.join();
    EXPECT_EQ(body_calls.load(), 0);
    tracker.WaitForDrain();
  });
}

TEST(OperationTrackerTest, G05EmptyClosedTrackerDrainsWithoutNotification) {
  RunBounded([] {
    Tracker tracker;
    tracker.OpenOnce();
    tracker.Close();
    tracker.WaitForDrain();
    EXPECT_FALSE(tracker.TryEnter().has_value());
  });
}

TEST(OperationTrackerTest, G06DrainPublishesEveryHeldLeaseTail) {
  RunBounded([] {
    Tracker tracker;
    tracker.OpenOnce();
    std::array<std::optional<Tracker::Lease>, 3> leases;
    for (auto& lease : leases) {
      lease = tracker.TryEnter();
      CHECK(lease.has_value());
    }
    tracker.Close();
    std::atomic<unsigned> completed{0};
    Checkpoint release;
    std::thread returner([&, leases = std::move(leases)]() mutable {
      release.Wait();
      for (auto& lease : leases) {
        completed.fetch_add(1, std::memory_order_relaxed);
        lease.reset();
      }
    });
    release.Hit();
    tracker.WaitForDrain();
    EXPECT_EQ(completed.load(std::memory_order_relaxed), 3);
    returner.join();
  });
}

TEST(OperationTrackerTest, G07MoveConstructionTransfersOneRelease) {
  RunBounded([] {
    Tracker tracker;
    tracker.OpenOnce();
    std::optional<Tracker::Lease> destination;
    {
      auto source = tracker.TryEnter();
      CHECK(source.has_value());
      destination.emplace(std::move(*source));
      EXPECT_FALSE(static_cast<bool>(*source));
      EXPECT_TRUE(static_cast<bool>(*destination));
    }
    tracker.Close();
    ReturnLeaseAndDrain(tracker, std::move(destination));
  });
}

TEST(OperationTrackerTest, G08MoveAssignmentAcrossTrackersReleasesOldLease) {
  RunBounded([] {
    Tracker first;
    Tracker second;
    first.OpenOnce();
    second.OpenOnce();
    auto destination = first.TryEnter();
    auto source = second.TryEnter();
    CHECK(destination.has_value());
    CHECK(source.has_value());
    *destination = std::move(*source);
    EXPECT_FALSE(static_cast<bool>(*source));
    EXPECT_TRUE(static_cast<bool>(*destination));
    source.reset();
    first.Close();
    first.WaitForDrain();
    second.Close();
    ReturnLeaseAndDrain(second, std::move(destination));
  });
}

TEST(OperationTrackerTest, G09LeaseReturnsOnAnotherThread) {
  RunBounded([] {
    Tracker tracker;
    tracker.OpenOnce();
    std::promise<std::optional<Tracker::Lease>> transferred;
    auto incoming = transferred.get_future();
    std::thread entrant([&] { transferred.set_value(tracker.TryEnter()); });
    CHECK(incoming.wait_for(kCheckpointTimeout) == std::future_status::ready);
    auto lease = incoming.get();
    CHECK(lease.has_value());
    entrant.join();

    Checkpoint admitted;
    Checkpoint release;
    std::atomic<bool> body_finished{false};
    std::thread returner([&, lease = std::move(lease)]() mutable {
      // Give the returning OS thread an admission of its own. Returning the
      // transferred lease must not consume this independent lease.
      auto local = tracker.TryEnter();
      CHECK(local.has_value());
      admitted.Hit();
      release.Wait();
      lease.reset();
      body_finished.store(true, std::memory_order_relaxed);
      local.reset();
    });
    admitted.Wait();
    tracker.Close();
    release.Hit();
    tracker.WaitForDrain();
    EXPECT_TRUE(body_finished.load(std::memory_order_relaxed));
    returner.join();
  });
}

TEST(OperationTrackerTest, G10NaturallyCollidingThreadsDrainEveryBody) {
  RunBounded(RunSlotContention);
}

TEST(OperationTrackerTest, G11CloseRacingEntrantsDrainsAcceptedBodies) {
  RunBounded([] {
    Tracker tracker;
    tracker.OpenOnce();
    constexpr unsigned kWorkers = 16;
    std::atomic<uint64_t> body_calls{0};
    Checkpoint admitted;
    Checkpoint race;
    std::vector<std::thread> entrants;
    entrants.reserve(kWorkers);
    for (unsigned i = 0; i < kWorkers; ++i) {
      entrants.emplace_back([&] {
        auto lease = tracker.TryEnter();
        CHECK(lease.has_value());
        admitted.Hit();
        race.Wait();
        do {
          body_calls.fetch_add(1, std::memory_order_relaxed);
          lease.reset();
          lease = tracker.TryEnter();
        } while (lease.has_value());
      });
    }
    admitted.Wait(kWorkers);
    race.Hit();
    tracker.Close();
    tracker.WaitForDrain();
    const auto drained_calls = body_calls.load(std::memory_order_relaxed);
    EXPECT_GE(drained_calls, kWorkers);
    EXPECT_FALSE(tracker.TryEnter().has_value());
    // Rejected callers may still have rollback/notification tails. Keep the
    // tracker alive until they join, and verify none ran an admitted body after
    // drain returned. The exact admission/Close interleaving is not forced.
    for (auto& entrant : entrants) entrant.join();
    EXPECT_EQ(body_calls.load(std::memory_order_relaxed), drained_calls);
  });
}

}  // namespace
}  // namespace client
}  // namespace dingofs
