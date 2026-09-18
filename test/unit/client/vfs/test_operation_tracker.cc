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
#include <limits>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

#include "client/vfs/operation_tracker.h"

#ifdef NDEBUG
#error "Tracker tests require a Debug build, like the existing SyncPoint tests"
#endif

namespace dingofs {
namespace client {

// Test-only access for exact CV handshakes and
// impossible-through-the-public-API invariant failures. Normal admission,
// release, and drain always use real code.
class OperationTrackerTestPeer {
 public:
  static std::unique_lock<std::mutex> LockDrain(OperationTracker& tracker) {
    return std::unique_lock<std::mutex>(tracker.drain_mutex_);
  }

  static void Notify(OperationTracker& tracker) {
    tracker.drain_cv_.notify_all();
  }

  static void SetCount(OperationTracker& tracker, unsigned slot,
                       uint64_t count) {
    tracker.counts_[slot].value.store(count, std::memory_order_seq_cst);
  }

  static void ReturnSlot(OperationTracker& tracker, unsigned slot) {
    tracker.Leave(slot);
  }
};

namespace {

using Tracker = OperationTracker;
enum class Point {
  kAfterFirstRead,
  kAfterIncrement,
  kAfterAdmission,
  kBeforeWait,
  kAfterDecrement,
  kBeforeNotify,
  kBeforeLeaseRelease,
};
constexpr auto kCheckpointTimeout = std::chrono::seconds(5);

// A missing wakeup cannot be repaired safely by destroying a tracker or
// detaching its callers. Contain each concurrency scenario in a death-test
// subprocess: success joins every thread; a bounded synchronization failure
// terminates the entire child, never leaving a live caller in the parent. The
// 20s watchdog is shorter than the production 30s diagnostic wakeup, so that
// wakeup cannot hide a missed notification. No timed negative observation
// proves a CV ordering.
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
  void Pause() {
    std::lock_guard<std::mutex> lock(mutex_);
    paused_ = true;
  }

  void Hit() {
    std::unique_lock<std::mutex> lock(mutex_);
    ++hits_;
    cv_.notify_all();
    CHECK(cv_.wait_for(lock, kCheckpointTimeout, [this] { return !paused_; }))
        << "Tracker checkpoint was not released";
  }

  void Wait(unsigned count = 1) {
    std::unique_lock<std::mutex> lock(mutex_);
    CHECK(cv_.wait_for(lock, kCheckpointTimeout,
                       [this, count] { return hits_ >= count; }))
        << "Tracker checkpoint not reached: expected " << count << ", got "
        << hits_;
  }

  void Resume() {
    std::lock_guard<std::mutex> lock(mutex_);
    paused_ = false;
    cv_.notify_all();
  }

  unsigned Hits() {
    std::lock_guard<std::mutex> lock(mutex_);
    return hits_;
  }

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  unsigned hits_{0};
  bool paused_{false};
};

class Hooks {
 public:
  Checkpoint& At(Point point) { return points_[static_cast<unsigned>(point)]; }

  void Attach(Tracker* tracker) {
    static const std::array<const char*, 7> names = {
        "OperationTracker::AfterFirstRead",
        "OperationTracker::AfterIncrement",
        "OperationTracker::AfterAdmission",
        "OperationTracker::BeforeWait",
        "OperationTracker::AfterDecrement",
        "OperationTracker::BeforeNotify",
        "OperationTracker::BeforeLeaseRelease",
    };
    auto* sync = SyncPoint::GetInstance();
    for (unsigned i = 0; i < names.size(); ++i) {
      sync->SetCallBack(names[i], [this, tracker, i](void* argument) {
        if (argument == tracker) points_[i].Hit();
      });
    }
    sync->EnableProcessing();
  }

  ~Hooks() {
    auto* sync = SyncPoint::GetInstance();
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  }

 private:
  std::array<Checkpoint, 7> points_;
};

class ForcedSlot {
 public:
  explicit ForcedSlot(unsigned slot) { Tracker::SetTestSlot(slot); }
  ~ForcedSlot() { Tracker::ClearTestSlot(); }
  ForcedSlot(const ForcedSlot&) = delete;
  ForcedSlot& operator=(const ForcedSlot&) = delete;
};

// Call only after a kBeforeWait observation, with at least one lease still
// held. Acquiring this mutex proves the waiter crossed the CV's atomic unlock;
// a marker outside WaitForDrain could not establish that boundary.
void AcknowledgeWait(Tracker& tracker, Hooks& hooks) {
  hooks.At(Point::kBeforeWait).Wait();
  auto lock = OperationTrackerTestPeer::LockDrain(tracker);
}

// Request a fresh predicate scan while a known lease is still held. The hit
// count is sampled under the drain mutex, so a subsequent hit cannot be an old
// scan. Spurious wakeups are harmless: either way a fresh nonzero predicate
// must be observed, rather than a timeout being used to claim non-completion.
void RequireAnotherWait(Tracker& tracker, Hooks& hooks) {
  unsigned next;
  {
    auto lock = OperationTrackerTestPeer::LockDrain(tracker);
    next = hooks.At(Point::kBeforeWait).Hits() + 1;
    OperationTrackerTestPeer::Notify(tracker);
  }
  hooks.At(Point::kBeforeWait).Wait(next);
}

void RunSlotContention(bool collide) {
  Tracker tracker;
  Hooks hooks;
  hooks.Attach(&tracker);
  tracker.OpenOnce();
  std::array<Checkpoint, Tracker::kSlotCount> admitted;
  std::array<Checkpoint, Tracker::kSlotCount> release;
  std::array<Checkpoint, Tracker::kSlotCount> returned;
  std::vector<std::thread> workers;
  workers.reserve(Tracker::kSlotCount);
  for (unsigned i = 0; i < Tracker::kSlotCount; ++i) {
    workers.emplace_back([&, i] {
      ForcedSlot slot(collide ? 0 : i);
      auto lease = tracker.TryEnter();
      CHECK(lease.has_value());
      admitted[i].Hit();
      release[i].Wait();
      lease.reset();
      returned[i].Hit();
    });
  }
  for (auto& checkpoint : admitted) checkpoint.Wait();
  tracker.Close();
  Checkpoint drained;
  std::thread waiter([&] {
    tracker.WaitForDrain();
    drained.Hit();
  });
  AcknowledgeWait(tracker, hooks);
  for (unsigned i = 0; i < Tracker::kSlotCount; ++i) {
    release[i].Hit();
    returned[i].Wait();
    if (i + 1 < Tracker::kSlotCount) {
      RequireAnotherWait(tracker, hooks);
      EXPECT_EQ(drained.Hits(), 0);
    }
  }
  drained.Wait();
  for (auto& worker : workers) worker.join();
  waiter.join();
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
  EXPECT_DEATH(
      {
        Tracker tracker;
        tracker.OpenOnce();
        tracker.OpenOnce();
      },
      "cannot reopen");
}

TEST(OperationTrackerDeathTest, G03ReopenAfterCloseFails) {
  EXPECT_DEATH(
      {
        Tracker tracker;
        tracker.OpenOnce();
        tracker.Close();
        tracker.OpenOnce();
      },
      "cannot reopen");
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

TEST(OperationTrackerTest, G06WaitsForEveryHeldLease) {
  RunBounded([] {
    Tracker tracker;
    Hooks hooks;
    hooks.Attach(&tracker);
    tracker.OpenOnce();
    std::array<std::optional<Tracker::Lease>, 3> leases;
    for (auto& lease : leases) {
      lease = tracker.TryEnter();
      CHECK(lease.has_value());
    }
    tracker.Close();
    Checkpoint drained;
    std::thread waiter([&] {
      tracker.WaitForDrain();
      drained.Hit();
    });
    AcknowledgeWait(tracker, hooks);
    for (unsigned i = 0; i < 2; ++i) {
      leases[i].reset();
      RequireAnotherWait(tracker, hooks);
      EXPECT_EQ(drained.Hits(), 0);
    }
    leases[2].reset();
    drained.Wait();
    waiter.join();
  });
}

TEST(OperationTrackerTest, G07MoveConstructionTransfersOneRelease) {
  RunBounded([] {
    Tracker tracker;
    Hooks hooks;
    hooks.Attach(&tracker);
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
    Checkpoint drained;
    std::thread waiter([&] {
      tracker.WaitForDrain();
      drained.Hit();
    });
    AcknowledgeWait(tracker, hooks);
    EXPECT_EQ(drained.Hits(), 0);
    destination.reset();
    drained.Wait();
    waiter.join();
  });
}

TEST(OperationTrackerTest, G08MoveAssignmentAcrossTrackersReleasesOldLease) {
  RunBounded([] {
    Tracker first;
    Tracker second;
    Hooks hooks;
    hooks.Attach(&second);
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
    Checkpoint drained;
    std::thread waiter([&] {
      second.WaitForDrain();
      drained.Hit();
    });
    AcknowledgeWait(second, hooks);
    EXPECT_EQ(drained.Hits(), 0);
    destination.reset();
    drained.Wait();
    waiter.join();
  });
}

TEST(OperationTrackerTest, G09ReturnOnAnotherThreadKeepsOriginalSlot) {
  RunBounded([] {
    Tracker tracker;
    Hooks hooks;
    hooks.Attach(&tracker);
    tracker.OpenOnce();
    std::promise<std::optional<Tracker::Lease>> transferred;
    auto incoming = transferred.get_future();
    std::thread entrant([&] {
      ForcedSlot slot(7);
      transferred.set_value(tracker.TryEnter());
    });
    CHECK(incoming.wait_for(kCheckpointTimeout) == std::future_status::ready);
    auto lease = incoming.get();
    CHECK(lease.has_value());
    entrant.join();
    ForcedSlot retained_slot(31);
    auto retained = tracker.TryEnter();
    CHECK(retained.has_value());
    tracker.Close();
    std::thread returner([lease = std::move(lease)]() mutable {
      ForcedSlot different_slot(31);
      lease.reset();
    });
    returner.join();
    Checkpoint drained;
    std::thread waiter([&] {
      tracker.WaitForDrain();
      drained.Hit();
    });
    AcknowledgeWait(tracker, hooks);
    EXPECT_EQ(drained.Hits(), 0);
    retained.reset();
    drained.Wait();
    waiter.join();
  });
}

TEST(OperationTrackerTest, G10SixtyFourThreadsCollideInOneSlot) {
  static_assert(Tracker::kSlotCount == 64,
                "Production tracker must stay fixed64");
  RunBounded([] { RunSlotContention(true); });
}

TEST(OperationTrackerTest, G11LateTentativeAfterDrainRejectsWithoutBody) {
  RunBounded([] {
    Tracker tracker;
    Hooks hooks;
    hooks.At(Point::kAfterFirstRead).Pause();
    hooks.Attach(&tracker);
    tracker.OpenOnce();
    unsigned body_calls = 0;
    std::thread entrant([&] {
      if (auto lease = tracker.TryEnter()) ++body_calls;
    });
    hooks.At(Point::kAfterFirstRead).Wait();
    tracker.Close();
    tracker.WaitForDrain();
    // Runtime teardown would now be allowed, but the owner still keeps the
    // tracker alive until this rejected caller's rollback and notify tail exit.
    hooks.At(Point::kAfterFirstRead).Resume();
    entrant.join();
    EXPECT_EQ(body_calls, 0);
    EXPECT_EQ(hooks.At(Point::kAfterIncrement).Hits(), 1);
    EXPECT_EQ(hooks.At(Point::kAfterDecrement).Hits(), 1);
  });
}

TEST(OperationTrackerTest, G12TentativeRollbackWakesDrainer) {
  RunBounded([] {
    Tracker tracker;
    Hooks hooks;
    hooks.At(Point::kAfterIncrement).Pause();
    hooks.Attach(&tracker);
    tracker.OpenOnce();
    unsigned body_calls = 0;
    std::thread entrant([&] {
      if (auto lease = tracker.TryEnter()) ++body_calls;
    });
    hooks.At(Point::kAfterIncrement).Wait();
    tracker.Close();
    Checkpoint drained;
    std::thread waiter([&] {
      tracker.WaitForDrain();
      drained.Hit();
    });
    AcknowledgeWait(tracker, hooks);
    hooks.At(Point::kAfterIncrement).Resume();
    drained.Wait();
    entrant.join();
    waiter.join();
    EXPECT_EQ(body_calls, 0);
  });
}

TEST(OperationTrackerTest, G13FinalReleaseBeforePredicateNeedsNoNotification) {
  RunBounded([] {
    Tracker tracker;
    Hooks hooks;
    hooks.At(Point::kBeforeNotify).Pause();
    hooks.Attach(&tracker);
    tracker.OpenOnce();
    auto lease = tracker.TryEnter();
    CHECK(lease.has_value());
    tracker.Close();
    std::thread returner(
        [lease = std::move(lease)]() mutable { lease.reset(); });
    hooks.At(Point::kBeforeNotify).Wait();
    // Counter is already zero; notification is still blocked. Scanning must
    // finish without entering CV wait or needing the delayed notification.
    tracker.WaitForDrain();
    EXPECT_EQ(hooks.At(Point::kBeforeWait).Hits(), 0);
    hooks.At(Point::kBeforeNotify).Resume();
    returner.join();
  });
}

TEST(OperationTrackerTest, G14FinalReleaseBetweenPredicateAndWaitIsNotLost) {
  RunBounded([] {
    Tracker tracker;
    Hooks hooks;
    hooks.At(Point::kBeforeWait).Pause();
    hooks.Attach(&tracker);
    tracker.OpenOnce();
    auto lease = tracker.TryEnter();
    CHECK(lease.has_value());
    tracker.Close();
    Checkpoint drained;
    std::thread waiter([&] {
      tracker.WaitForDrain();
      drained.Hit();
    });
    hooks.At(Point::kBeforeWait).Wait();
    // The waiter has seen nonzero and STILL HOLDS drain_mutex_. Decrement is
    // now forced before the CV atomically drops that mutex and enters wait.
    std::thread returner(
        [lease = std::move(lease)]() mutable { lease.reset(); });
    hooks.At(Point::kBeforeNotify).Wait();
    EXPECT_EQ(drained.Hits(), 0);
    hooks.At(Point::kBeforeWait).Resume();
    drained.Wait();
    returner.join();
    waiter.join();
  });
}

TEST(OperationTrackerTest, G15FinalReleaseAfterWaitWakesDrainer) {
  RunBounded([] {
    Tracker tracker;
    Hooks hooks;
    hooks.Attach(&tracker);
    tracker.OpenOnce();
    auto lease = tracker.TryEnter();
    CHECK(lease.has_value());
    tracker.Close();
    Checkpoint drained;
    std::thread waiter([&] {
      tracker.WaitForDrain();
      drained.Hit();
    });
    // Tracker mutex acquisition acknowledges actual CV unlock, not merely entry
    // into WaitForDrain. The last release only happens after that handshake.
    AcknowledgeWait(tracker, hooks);
    EXPECT_EQ(drained.Hits(), 0);
    lease.reset();
    drained.Wait();
    waiter.join();
  });
}

TEST(OperationTrackerTest, G16TentativeRollbackCannotCancelTwoAdmittedLeases) {
  RunBounded([] {
    Tracker tracker;
    Hooks hooks;
    hooks.Attach(&tracker);
    tracker.OpenOnce();
    ForcedSlot slot(0);
    auto first = tracker.TryEnter();
    auto second = tracker.TryEnter();
    CHECK(first.has_value());
    CHECK(second.has_value());
    hooks.At(Point::kAfterIncrement).Pause();
    unsigned body_calls = 0;
    std::thread tentative([&] {
      ForcedSlot collision(0);
      if (auto lease = tracker.TryEnter()) ++body_calls;
    });
    hooks.At(Point::kAfterIncrement).Wait(3);
    tracker.Close();
    Checkpoint drained;
    std::thread waiter([&] {
      tracker.WaitForDrain();
      drained.Hit();
    });
    AcknowledgeWait(tracker, hooks);
    hooks.At(Point::kAfterIncrement).Resume();
    tentative.join();
    EXPECT_EQ(body_calls, 0);
    RequireAnotherWait(tracker, hooks);
    EXPECT_EQ(drained.Hits(), 0);
    first.reset();
    RequireAnotherWait(tracker, hooks);
    EXPECT_EQ(drained.Hits(), 0);
    second.reset();
    drained.Wait();
    waiter.join();
  });
}

TEST(OperationTrackerTest, G17DrainScansEverySlot) {
  RunBounded([] { RunSlotContention(false); });
}

TEST(OperationTrackerDeathTest, G18CounterOverflowFails) {
  EXPECT_DEATH(
      {
        Tracker tracker;
        ForcedSlot slot(0);
        tracker.OpenOnce();
        OperationTrackerTestPeer::SetCount(
            tracker, 0, std::numeric_limits<uint64_t>::max());
        tracker.TryEnter();
      },
      "counter overflow");
}

TEST(OperationTrackerDeathTest, G18CounterUnderflowFails) {
  EXPECT_DEATH(
      {
        Tracker tracker;
        OperationTrackerTestPeer::ReturnSlot(tracker, 0);
      },
      "underflow or double release");
}

TEST(OperationTrackerDeathTest, G18DuplicateReturnFails) {
  EXPECT_DEATH(([] {
                 Tracker tracker;
                 ForcedSlot slot(0);
                 tracker.OpenOnce();
                 auto lease = tracker.TryEnter();
                 CHECK(lease.has_value());
                 lease.reset();
                 // Simulate a duplicated ownership token without invoking a C++
                 // object destructor twice (which would itself be undefined
                 // behavior).
                 OperationTrackerTestPeer::ReturnSlot(tracker, 0);
               }()),
               "underflow or double release");
}

}  // namespace
}  // namespace client
}  // namespace dingofs
