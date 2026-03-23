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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <fcntl.h>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "client/vfs/handle/handle_manager.h"
#include "test/unit/client/vfs/mock/mock_ifile.h"
#include "test/unit/client/vfs/test_base.h"

namespace dingofs {
namespace client {
namespace vfs {

using ::testing::AnyNumber;
using ::testing::Return;
using dingofs::client::vfs::test::MockIFile;

class HandleManagerTest : public dingofs::client::vfs::test::VFSTestBase {
 protected:
  // Helper to create a MockIFile with default permissive behavior.
  IFileUPtr MakeMockIFile() {
    auto f = std::make_unique<MockIFile>();
    ON_CALL(*f, Open()).WillByDefault(Return(Status::OK()));
    ON_CALL(*f, Flush()).WillByDefault(Return(Status::OK()));
    ON_CALL(*f, Close()).WillByDefault(Return());
    ON_CALL(*f, Invalidate(::testing::_, ::testing::_))
        .WillByDefault(Return());
    EXPECT_CALL(*f, Open()).Times(AnyNumber());
    EXPECT_CALL(*f, Flush()).Times(AnyNumber());
    EXPECT_CALL(*f, Close()).Times(AnyNumber());
    EXPECT_CALL(*f, Invalidate(::testing::_, ::testing::_))
        .Times(AnyNumber());
    return f;
  }
};

// 1. NewHandle creates a handle with the correct ino/fh/flags.
TEST_F(HandleManagerTest, NewHandle_FieldsCorrect) {
  auto f = MakeMockIFile();
  Handle* h = handle_manager_->NewHandle(10, 100, O_RDONLY, std::move(f));
  ASSERT_NE(h, nullptr);
  EXPECT_EQ(h->fh, 10u);
  EXPECT_EQ(h->ino, 100u);
  EXPECT_EQ(h->flags, O_RDONLY);

  // Clean up: release the manager's ref so the handle is freed.
  handle_manager_->ReleaseHandler(10);
}

// 2. FindHandler hits on a registered handle and returns nullptr for unknown fh.
TEST_F(HandleManagerTest, FindHandler_Hit_And_Miss) {
  auto f = MakeMockIFile();
  Handle* h = handle_manager_->NewHandle(20, 200, O_RDWR, std::move(f));
  ASSERT_NE(h, nullptr);

  Handle* found = handle_manager_->FindHandler(20);
  EXPECT_EQ(found, h);

  Handle* miss = handle_manager_->FindHandler(9999);
  EXPECT_EQ(miss, nullptr);

  handle_manager_->ReleaseHandler(20);
}

// 3. ReleaseHandler removes the handle so subsequent FindHandler returns nullptr.
TEST_F(HandleManagerTest, ReleaseHandler_RemovesFromMap) {
  auto f = MakeMockIFile();
  handle_manager_->NewHandle(30, 300, O_RDONLY, std::move(f));

  handle_manager_->ReleaseHandler(30);

  EXPECT_EQ(handle_manager_->FindHandler(30), nullptr);
}

// 4. AcquireRef/ReleaseRef lifecycle: refs correctly tracks reference count.
TEST_F(HandleManagerTest, AcquireRef_ReleaseRef_Lifecycle) {
  auto f = MakeMockIFile();
  // NewHandle internally calls AddHandle which calls AcquireRef (refs -> 1).
  Handle* h = handle_manager_->NewHandle(40, 400, O_RDWR, std::move(f));
  ASSERT_NE(h, nullptr);
  EXPECT_EQ(h->refs.load(), 1);

  // Extra acquire.
  h->AcquireRef();
  EXPECT_EQ(h->refs.load(), 2);

  // Release extra ref; handle should still be alive (refs = 1).
  h->ReleaseRef();
  EXPECT_EQ(h->refs.load(), 1);

  // Let the manager release its own ref (this deletes h).
  handle_manager_->ReleaseHandler(40);
  // h is now a dangling pointer — do not dereference.
}

// 5. FlushByIno only flushes handles matching the given ino.
TEST_F(HandleManagerTest, FlushByIno_OnlyMatchingIno) {
  auto f1 = std::make_unique<MockIFile>();
  auto* f1_ptr = f1.get();
  ON_CALL(*f1_ptr, Open()).WillByDefault(Return(Status::OK()));
  ON_CALL(*f1_ptr, Flush()).WillByDefault(Return(Status::OK()));
  ON_CALL(*f1_ptr, Close()).WillByDefault(Return());
  ON_CALL(*f1_ptr, Invalidate(::testing::_, ::testing::_))
      .WillByDefault(Return());
  EXPECT_CALL(*f1_ptr, Flush()).Times(1);
  EXPECT_CALL(*f1_ptr, Open()).Times(AnyNumber());
  EXPECT_CALL(*f1_ptr, Close()).Times(AnyNumber());
  EXPECT_CALL(*f1_ptr, Invalidate(::testing::_, ::testing::_))
      .Times(AnyNumber());

  auto f2 = std::make_unique<MockIFile>();
  auto* f2_ptr = f2.get();
  ON_CALL(*f2_ptr, Open()).WillByDefault(Return(Status::OK()));
  ON_CALL(*f2_ptr, Flush()).WillByDefault(Return(Status::OK()));
  ON_CALL(*f2_ptr, Close()).WillByDefault(Return());
  ON_CALL(*f2_ptr, Invalidate(::testing::_, ::testing::_))
      .WillByDefault(Return());
  EXPECT_CALL(*f2_ptr, Flush()).Times(0);
  EXPECT_CALL(*f2_ptr, Open()).Times(AnyNumber());
  EXPECT_CALL(*f2_ptr, Close()).Times(AnyNumber());
  EXPECT_CALL(*f2_ptr, Invalidate(::testing::_, ::testing::_))
      .Times(AnyNumber());

  handle_manager_->NewHandle(51, 100, O_RDWR, std::move(f1));
  handle_manager_->NewHandle(52, 200, O_RDWR, std::move(f2));

  Status s = handle_manager_->FlushByIno(100);
  EXPECT_TRUE(s.ok());

  handle_manager_->ReleaseHandler(51);
  handle_manager_->ReleaseHandler(52);
}

// 6. FlushByIno flushes all handles sharing the same ino.
TEST_F(HandleManagerTest, FlushByIno_AllHandlesOfSameIno) {
  constexpr Ino kIno = 300;

  auto make_counted_file = [&]() -> std::pair<IFileUPtr, MockIFile*> {
    auto f = std::make_unique<MockIFile>();
    auto* ptr = f.get();
    ON_CALL(*ptr, Flush()).WillByDefault(Return(Status::OK()));
    ON_CALL(*ptr, Close()).WillByDefault(Return());
    ON_CALL(*ptr, Open()).WillByDefault(Return(Status::OK()));
    ON_CALL(*ptr, Invalidate(::testing::_, ::testing::_))
        .WillByDefault(Return());
    EXPECT_CALL(*ptr, Flush()).Times(1);
    EXPECT_CALL(*ptr, Open()).Times(AnyNumber());
    EXPECT_CALL(*ptr, Close()).Times(AnyNumber());
    EXPECT_CALL(*ptr, Invalidate(::testing::_, ::testing::_))
        .Times(AnyNumber());
    return {std::move(f), ptr};
  };

  auto [f1, _p1] = make_counted_file();
  auto [f2, _p2] = make_counted_file();
  auto [f3, _p3] = make_counted_file();

  handle_manager_->NewHandle(61, kIno, O_RDWR, std::move(f1));
  handle_manager_->NewHandle(62, kIno, O_RDWR, std::move(f2));
  handle_manager_->NewHandle(63, kIno, O_RDWR, std::move(f3));

  Status s = handle_manager_->FlushByIno(kIno);
  EXPECT_TRUE(s.ok());

  handle_manager_->ReleaseHandler(61);
  handle_manager_->ReleaseHandler(62);
  handle_manager_->ReleaseHandler(63);
}

// 7. FlushByIno releases the mutex while calling Flush so other threads can
//    AddHandle concurrently — no deadlock.
TEST_F(HandleManagerTest, FlushByIno_OutsideLock_NoDeadlock) {
  std::mutex m;
  std::condition_variable cv;
  bool flushing = false;

  auto slow_file = std::make_unique<MockIFile>();
  auto* slow_ptr = slow_file.get();
  ON_CALL(*slow_ptr, Open()).WillByDefault(Return(Status::OK()));
  ON_CALL(*slow_ptr, Close()).WillByDefault(Return());
  ON_CALL(*slow_ptr, Invalidate(::testing::_, ::testing::_))
      .WillByDefault(Return());
  ON_CALL(*slow_ptr, Flush()).WillByDefault([&]() -> Status {
    {
      std::lock_guard<std::mutex> lk(m);
      flushing = true;
    }
    cv.notify_all();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    return Status::OK();
  });
  EXPECT_CALL(*slow_ptr, Flush()).Times(1);
  EXPECT_CALL(*slow_ptr, Open()).Times(AnyNumber());
  EXPECT_CALL(*slow_ptr, Close()).Times(AnyNumber());
  EXPECT_CALL(*slow_ptr, Invalidate(::testing::_, ::testing::_))
      .Times(AnyNumber());

  handle_manager_->NewHandle(71, 100, O_RDWR, std::move(slow_file));

  std::thread flush_thread(
      [&] { handle_manager_->FlushByIno(100); });

  // Wait until flush has started (lock is released).
  {
    std::unique_lock<std::mutex> lk(m);
    cv.wait(lk, [&] { return flushing; });
  }

  // This must not deadlock: AddHandle acquires the same mutex.
  auto f2 = MakeMockIFile();
  handle_manager_->NewHandle(72, 200, O_RDWR, std::move(f2));

  flush_thread.join();

  handle_manager_->ReleaseHandler(71);
  handle_manager_->ReleaseHandler(72);
}

// 8. InvalidateByIno calls Invalidate on the matching handle with correct
//    offset and size.
TEST_F(HandleManagerTest, InvalidateByIno_CorrectRangeAndIno) {
  constexpr int64_t kOffset = 4096;
  constexpr int64_t kSize = 8192;

  auto f = std::make_unique<MockIFile>();
  auto* fptr = f.get();
  ON_CALL(*fptr, Open()).WillByDefault(Return(Status::OK()));
  ON_CALL(*fptr, Flush()).WillByDefault(Return(Status::OK()));
  ON_CALL(*fptr, Close()).WillByDefault(Return());
  EXPECT_CALL(*fptr, Invalidate(kOffset, kSize)).Times(1);
  EXPECT_CALL(*fptr, Open()).Times(AnyNumber());
  EXPECT_CALL(*fptr, Flush()).Times(AnyNumber());
  EXPECT_CALL(*fptr, Close()).Times(AnyNumber());

  handle_manager_->NewHandle(80, 500, O_RDWR, std::move(f));
  handle_manager_->InvalidateByIno(500, kOffset, kSize);
  handle_manager_->ReleaseHandler(80);
}

// 9. Concurrent AddHandle from 20 threads — all handles are findable afterward.
TEST_F(HandleManagerTest, Concurrent_AddAndFind) {
  constexpr int kThreads = 20;
  std::vector<std::thread> threads;
  threads.reserve(kThreads);

  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&, i]() {
      uint64_t fh = 1000 + i;
      auto f = MakeMockIFile();
      handle_manager_->NewHandle(fh, static_cast<Ino>(fh), O_RDONLY,
                                 std::move(f));
    });
  }
  for (auto& t : threads) {
    t.join();
  }

  for (int i = 0; i < kThreads; ++i) {
    uint64_t fh = 1000 + i;
    Handle* h = handle_manager_->FindHandler(fh);
    EXPECT_NE(h, nullptr) << "missing fh=" << fh;
  }

  for (int i = 0; i < kThreads; ++i) {
    handle_manager_->ReleaseHandler(1000 + i);
  }
}

// 10. Concurrent FlushByIno and ReleaseHandler — no crash or assertion failure.
TEST_F(HandleManagerTest, Concurrent_FlushAndRelease_NoCrash) {
  constexpr int kHandles = 10;
  for (int i = 0; i < kHandles; ++i) {
    auto f = MakeMockIFile();
    handle_manager_->NewHandle(2000 + i, 700, O_RDWR, std::move(f));
  }

  std::thread flush_thread([&] {
    for (int i = 0; i < 5; ++i) {
      handle_manager_->FlushByIno(700);
    }
  });

  std::thread release_thread([&] {
    for (int i = 0; i < kHandles; ++i) {
      handle_manager_->ReleaseHandler(2000 + i);
    }
  });

  flush_thread.join();
  release_thread.join();
  // No crash → test passes.
}

// 11. Stop closes all non-stats handles; subsequent operations are safe.
TEST_F(HandleManagerTest, Stop_ClosesAllHandles) {
  constexpr int kHandles = 5;
  for (int i = 0; i < kHandles; ++i) {
    auto f = std::make_unique<MockIFile>();
    auto* fptr = f.get();
    ON_CALL(*fptr, Open()).WillByDefault(Return(Status::OK()));
    ON_CALL(*fptr, Flush()).WillByDefault(Return(Status::OK()));
    ON_CALL(*fptr, Close()).WillByDefault(Return());
    ON_CALL(*fptr, Invalidate(::testing::_, ::testing::_))
        .WillByDefault(Return());
    // Close must be called exactly once per handle during Stop.
    EXPECT_CALL(*fptr, Close()).Times(1);
    EXPECT_CALL(*fptr, Open()).Times(AnyNumber());
    EXPECT_CALL(*fptr, Flush()).Times(AnyNumber());
    EXPECT_CALL(*fptr, Invalidate(::testing::_, ::testing::_))
        .Times(AnyNumber());

    handle_manager_->NewHandle(3000 + i, static_cast<Ino>(3000 + i), O_RDWR,
                               std::move(f));
  }

  // Stop is also called by VFSTestBase dtor, but calling it twice is safe.
  handle_manager_->Stop();

  // After Stop, FlushByIno returns OK without doing anything.
  Status s = handle_manager_->FlushByIno(3000);
  EXPECT_TRUE(s.ok());
}

// 12. Dump serialises handle data; Load restores it without crashing.
//     (Load creates real File objects so we only test Dump round-trip here.)
TEST_F(HandleManagerTest, Dump_DoesNotCrash) {
  auto f = MakeMockIFile();
  handle_manager_->NewHandle(4000, 800, O_RDWR, std::move(f));

  Json::Value root;
  bool ok = handle_manager_->Dump(root);
  EXPECT_TRUE(ok);

  // Verify basic structure.
  ASSERT_TRUE(root.isMember("handlers"));
  ASSERT_TRUE(root["handlers"].isArray());
  EXPECT_GE(root["handlers"].size(), 1u);

  // Check the dumped entry contains required fields.
  bool found = false;
  for (const auto& item : root["handlers"]) {
    if (item["fh"].asUInt64() == 4000u) {
      EXPECT_EQ(item["ino"].asUInt64(), 800u);
      found = true;
    }
  }
  EXPECT_TRUE(found);

  handle_manager_->ReleaseHandler(4000);
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
