/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <mutex>
#include <thread>
#include <vector>

#include "common/writemempool/write_mem_pool.h"

namespace dingofs {

class WriteMemPoolTestPeer {
 public:
  static char* RemovePhysicalPage(WriteMemPool* pool) {
    char* page = nullptr;
    CHECK_EQ(pool->page_pool_->RequireBatch(&page, 1), 1);
    return page;
  }

  static void ReturnPhysicalPage(WriteMemPool* pool, char* page) {
    pool->page_pool_->ReleaseBatch(&page, 1);
  }
};
namespace {

using namespace std::chrono_literals;

class CountingObserver : public WritePressureObserver {
 public:
  void OnWritePressure() override {
    std::lock_guard<std::mutex> lock(mutex_);
    ++event_count_;
    cv_.notify_all();
  }

  bool WaitForEvent() {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, 5s, [this] { return event_count_ != 0; });
  }

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  int event_count_{0};
};

class BlockingObserver : public WritePressureObserver {
 public:
  void OnWritePressure() override {
    std::unique_lock<std::mutex> lock(mutex_);
    entered_ = true;
    cv_.notify_all();
    cv_.wait(lock, [this] { return released_; });
  }

  bool WaitUntilEntered() {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, 5s, [this] { return entered_; });
  }

  void Release() {
    std::lock_guard<std::mutex> lock(mutex_);
    released_ = true;
    cv_.notify_all();
  }

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  bool entered_{false};
  bool released_{false};
};

TEST(WriteMemPoolDeathTest, RejectsZeroPageSizeBeforeDivision) {
  EXPECT_DEATH({ WriteMemPool pool(4096, 0); }, "page_size > 0");
}

TEST(WriteMemPoolDeathTest, RejectsCapacitySmallerThanOnePage) {
  EXPECT_DEATH({ WriteMemPool pool(4096, 8192); }, "total_bytes >= page_size");
}

TEST(WriteMemPoolDeathTest, RejectsNonIntegralPageCapacity) {
  EXPECT_DEATH({ WriteMemPool pool(10 * 1024, 4096); }, "exact multiple");
}

TEST(WriteMemPoolTest, GeometryAndLeaseAccounting) {
  constexpr int64_t kPage = 4096;
  WriteMemPool pool(kPage * 4, kPage);
  EXPECT_EQ(pool.GetPageSize(), kPage);
  EXPECT_EQ(pool.GetTotalBytes(), kPage * 4);
  EXPECT_EQ(pool.BufferCount(), 4);
  EXPECT_EQ(pool.TotalSize(), kPage * 4);
  EXPECT_NE(pool.BaseAddr(), nullptr);

  {
    WritePageLease lease;
    ASSERT_TRUE(pool.Acquire(2, &lease).ok());
    EXPECT_EQ(lease.Size(), 2);
    EXPECT_EQ(pool.GetUsedBytes(), kPage * 2);
  }
  EXPECT_EQ(pool.GetUsedBytes(), 0);
}

TEST(WriteMemPoolTest, TakeTransfersPageOwnership) {
  constexpr int64_t kPage = 4096;
  WriteMemPool pool(kPage * 2, kPage);
  char* page = nullptr;
  {
    WritePageLease lease;
    ASSERT_TRUE(pool.Acquire(1, &lease).ok());
    lease.Take(1, &page);
    EXPECT_TRUE(lease.Empty());
  }
  EXPECT_EQ(pool.GetUsedBytes(), kPage);
  pool.Release(&page, 1);
  EXPECT_EQ(pool.GetUsedBytes(), 0);
}

TEST(WriteMemPoolTest, RemovingObserverWaitsForInFlightNotification) {
  constexpr int64_t kPage = 4096;
  WriteMemPool pool(kPage, kPage);
  BlockingObserver observer;
  pool.SetPressureObserver(&observer);

  WritePageLease held;
  ASSERT_TRUE(pool.Acquire(1, &held).ok());
  auto blocked = std::async(std::launch::async, [&] {
    WritePageLease lease;
    return pool.Acquire(1, &lease);
  });
  if (!observer.WaitUntilEntered()) {
    observer.Release();
    pool.Close();
    FAIL() << "pressure notification did not start";
  }

  std::promise<void> unregister_started;
  auto unregister = std::async(std::launch::async, [&] {
    unregister_started.set_value();
    pool.SetPressureObserver(nullptr);
  });
  unregister_started.get_future().wait();
  EXPECT_EQ(unregister.wait_for(50ms), std::future_status::timeout);

  observer.Release();
  EXPECT_EQ(unregister.wait_for(5s), std::future_status::ready);
  unregister.get();

  pool.Close();
  EXPECT_TRUE(blocked.get().IsStop());
}

TEST(WriteMemPoolTest, TryAcquireIsExactAndDoesNotBypassWaiter) {
  constexpr int64_t kPage = 4096;
  WriteMemPool pool(kPage * 2, kPage);
  CountingObserver observer;
  pool.SetPressureObserver(&observer);

  WritePageLease held;
  ASSERT_TRUE(pool.Acquire(2, &held).ok());

  auto blocked = std::async(std::launch::async, [&] {
    WritePageLease lease;
    return pool.Acquire(2, &lease);
  });
  ASSERT_TRUE(observer.WaitForEvent());

  WritePageLease opportunistic;
  Status s = pool.TryAcquire(1, &opportunistic);
  EXPECT_TRUE(s.IsNotFit()) << s.ToString();

  held = WritePageLease();
  EXPECT_TRUE(blocked.get().ok());
  pool.SetPressureObserver(nullptr);
}

TEST(WriteMemPoolTest, FifoHeadIsNotBypassedBySmallerRequest) {
  constexpr int64_t kPage = 4096;
  WriteMemPool pool(kPage * 3, kPage);
  CountingObserver observer;
  pool.SetPressureObserver(&observer);

  WritePageLease initial;
  ASSERT_TRUE(pool.Acquire(3, &initial).ok());
  std::array<char*, 3> pages{};
  initial.Take(pages.size(), pages.data());

  std::promise<void> first_acquired;
  auto first_acquired_future = first_acquired.get_future();
  std::promise<void> release_first;
  auto release_first_future = release_first.get_future();
  auto first = std::async(std::launch::async, [&] {
    WritePageLease lease;
    Status s = pool.Acquire(2, &lease);
    first_acquired.set_value();
    release_first_future.wait();
    return s;
  });
  ASSERT_TRUE(observer.WaitForEvent());

  auto second = std::async(std::launch::async, [&] {
    WritePageLease lease;
    return pool.Acquire(1, &lease);
  });

  pool.Release(pages.data(), 1);
  EXPECT_EQ(second.wait_for(50ms), std::future_status::timeout);

  pool.Release(pages.data() + 1, 1);
  ASSERT_EQ(first_acquired_future.wait_for(5s), std::future_status::ready);
  EXPECT_EQ(second.wait_for(50ms), std::future_status::timeout);
  release_first.set_value();
  EXPECT_TRUE(first.get().ok());
  EXPECT_TRUE(second.get().ok());

  pool.Release(pages.data() + 2, 1);
  pool.SetPressureObserver(nullptr);
}

TEST(WriteMemPoolTest, CloseWakesQueuedAcquireAndRejectsNewAdmission) {
  constexpr int64_t kPage = 4096;
  WriteMemPool pool(kPage, kPage);
  CountingObserver observer;
  pool.SetPressureObserver(&observer);
  WritePageLease held;
  ASSERT_TRUE(pool.Acquire(1, &held).ok());

  auto blocked = std::async(std::launch::async, [&] {
    WritePageLease lease;
    return pool.Acquire(1, &lease);
  });
  ASSERT_TRUE(observer.WaitForEvent());
  pool.Close();

  Status waiting = blocked.get();
  EXPECT_TRUE(waiting.IsStop()) << waiting.ToString();
  WritePageLease rejected;
  EXPECT_TRUE(pool.Acquire(1, &rejected).IsStop());
}

TEST(WriteMemPoolTest, OversizedRequestsFailWithoutQueueing) {
  WriteMemPool pool(2 * 4096, 4096);
  WritePageLease lease;
  EXPECT_TRUE(pool.Acquire(3, &lease).IsNoSpace());
  EXPECT_TRUE(pool.TryAcquire(3, &lease).IsNotFit());
  EXPECT_FALSE(pool.IsPressured());
}

TEST(WriteMemPoolTest, ConcurrentAcquireAndReleaseReturnsToZero) {
  constexpr int kThreads = 16;
  constexpr int kIterations = 2000;
  constexpr int64_t kPage = 4096;
  WriteMemPool pool(kThreads * kPage, kPage);

  std::vector<std::thread> threads;
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&] {
      for (int j = 0; j < kIterations; ++j) {
        WritePageLease lease;
        Status status = pool.Acquire(1, &lease);
        ASSERT_TRUE(status.ok()) << status.ToString();
      }
    });
  }
  for (auto& thread : threads) thread.join();
  EXPECT_EQ(pool.GetUsedBytes(), 0);
}

TEST(WriteMemPoolTest, ReleaseAfterCloseRestoresAccounting) {
  constexpr int64_t kPage = 4096;
  WriteMemPool pool(2 * kPage, kPage);
  WritePageLease lease;
  ASSERT_TRUE(pool.Acquire(2, &lease).ok());
  std::array<char*, 2> pages{};
  lease.Take(pages.size(), pages.data());

  pool.Close();
  pool.Release(pages.data(), pages.size());

  EXPECT_EQ(pool.GetUsedBytes(), 0);
  WritePageLease rejected;
  EXPECT_TRUE(pool.TryAcquire(1, &rejected).IsStop());
}

TEST(WriteMemPoolTest, StablePhysicalShortBreaksPool) {
  constexpr int64_t kPage = 4096;
  WriteMemPool pool(2 * kPage, kPage);
  char* stolen = WriteMemPoolTestPeer::RemovePhysicalPage(&pool);

  WritePageLease lease;
  Status status = pool.Acquire(2, &lease);
  EXPECT_TRUE(status.IsInternal()) << status.ToString();
  EXPECT_TRUE(lease.Empty());
  EXPECT_EQ(pool.GetUsedBytes(), 0);

  WritePageLease rejected;
  EXPECT_TRUE(pool.Acquire(1, &rejected).IsInternal());
  WriteMemPoolTestPeer::ReturnPhysicalPage(&pool, stolen);
}

}  // namespace
}  // namespace dingofs
