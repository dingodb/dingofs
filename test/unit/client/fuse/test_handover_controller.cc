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

#include "client/fuse/upgrade/handover_controller.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>

#include "client/fuse/upgrade/handover_peer.h"
#include "client/fuse/upgrade/handover_session.h"
#include "client/fuse/upgrade/state_store.h"
#include "common/status.h"

namespace dingofs {
namespace client {
namespace fuse {

using ::testing::_;
using ::testing::InSequence;
using ::testing::InvokeWithoutArgs;
using ::testing::Return;

namespace {
constexpr auto kWaitTimeout = std::chrono::seconds(5);
}  // namespace

class MockHandoverPeer : public HandoverPeer {
 public:
  MOCK_METHOD(bool, WaitHandoverPrepare, (), (override));
  MOCK_METHOD(bool, NotifyReadyToExit, (), (override));
  MOCK_METHOD(void, NotifyHandoverAbort, (), (override));
};

class MockHandoverSession : public HandoverSession {
 public:
  MOCK_METHOD(void, PauseReceive, (), (override));
  MOCK_METHOD(void, ResumeReceive, (), (override));
  MOCK_METHOD(int, WaitDrained, (uint32_t), (override));
  MOCK_METHOD(void, Exit, (), (override));
};

// A one-shot latch the test waits on; a mock action fulfils it once the
// controller reaches the expected terminal step.
class Latch {
 public:
  void Signal() { promise_.set_value(); }
  bool Wait() {
    return future_.wait_for(kWaitTimeout) == std::future_status::ready;
  }

 private:
  std::promise<void> promise_;
  std::future<void> future_{promise_.get_future()};
};

class HandoverControllerTest : public ::testing::Test {
 protected:
  void SetUp() override { ResetState(); }
  void TearDown() override { ResetState(); }

  static void ResetState() {
    UpgradeStateStore::GetInstance().UpdateFuseState(
        FuseUpgradeState::kFuseNormal);
  }
  static FuseUpgradeState State() {
    return UpgradeStateStore::GetInstance().GetFuseState();
  }

  HandoverOptions Options() {
    HandoverOptions o;
    o.mountpoint = "/tmp/handover-controller-test-no-such-mount";
    o.drain_timeout_ms = 100;
    o.statfs_interval_ms = 10;
    return o;
  }

  MockHandoverPeer peer_;
  MockHandoverSession session_;
};

// Full success: drain -> checkpoint -> ready/ACK -> exit. No resume/abort, and
// the state stays kFuseUpgradeOld (the new takes over).
TEST_F(HandoverControllerTest, HappyPath_DrainCheckpointCommitExits) {
  Latch done;
  std::atomic<bool> checkpoint_ran{false};

  {
    InSequence seq;
    EXPECT_CALL(peer_, WaitHandoverPrepare()).WillOnce(Return(true));
    EXPECT_CALL(session_, PauseReceive());
    EXPECT_CALL(session_, WaitDrained(100)).WillOnce(Return(0));
    EXPECT_CALL(peer_, NotifyReadyToExit()).WillOnce(Return(true));
    EXPECT_CALL(session_, Exit()).WillOnce(InvokeWithoutArgs([&] {
      done.Signal();
    }));
  }
  EXPECT_CALL(session_, ResumeReceive()).Times(0);
  EXPECT_CALL(peer_, NotifyHandoverAbort()).Times(0);

  HandoverController controller(Options(), &peer_, &session_);
  controller.SetCheckpoint([&]() -> Status {
    checkpoint_ran.store(true);
    return Status::OK();
  });
  controller.Start();
  ASSERT_TRUE(controller.RequestHandover());

  ASSERT_TRUE(done.Wait());
  controller.Stop();

  EXPECT_TRUE(checkpoint_ran.load());
  EXPECT_EQ(State(), FuseUpgradeState::kFuseUpgradeOld);
}

// Drain times out (WaitDrained != 0): abort, resume serving, roll state back.
// The checkpoint must NOT run and the session must NOT exit.
TEST_F(HandoverControllerTest, DrainTimeout_AbortsAndResumes) {
  Latch done;
  std::atomic<bool> checkpoint_ran{false};

  {
    InSequence seq;
    EXPECT_CALL(peer_, WaitHandoverPrepare()).WillOnce(Return(true));
    EXPECT_CALL(session_, PauseReceive());
    EXPECT_CALL(session_, WaitDrained(_)).WillOnce(Return(-1));
    EXPECT_CALL(peer_, NotifyHandoverAbort());
    EXPECT_CALL(session_, ResumeReceive()).WillOnce(InvokeWithoutArgs([&] {
      done.Signal();
    }));
  }
  EXPECT_CALL(session_, Exit()).Times(0);
  EXPECT_CALL(peer_, NotifyReadyToExit()).Times(0);

  HandoverController controller(Options(), &peer_, &session_);
  controller.SetCheckpoint([&]() -> Status {
    checkpoint_ran.store(true);
    return Status::OK();
  });
  controller.Start();
  controller.RequestHandover();

  ASSERT_TRUE(done.Wait());
  controller.Stop();

  EXPECT_FALSE(checkpoint_ran.load());
  EXPECT_EQ(State(), FuseUpgradeState::kFuseNormal);
}

// M1: a SIGHUP before the checkpoint is registered must NOT pause IO (no drain).
// It aborts immediately (telling the new to back off) and keeps serving.
TEST_F(HandoverControllerTest, NoCheckpoint_AbortsWithoutDraining) {
  Latch done;

  EXPECT_CALL(peer_, WaitHandoverPrepare()).WillOnce(Return(true));
  EXPECT_CALL(peer_, NotifyHandoverAbort()).WillOnce(InvokeWithoutArgs([&] {
    done.Signal();
  }));
  EXPECT_CALL(session_, PauseReceive()).Times(0);
  EXPECT_CALL(session_, WaitDrained(_)).Times(0);
  EXPECT_CALL(session_, ResumeReceive()).Times(0);
  EXPECT_CALL(session_, Exit()).Times(0);

  HandoverController controller(Options(), &peer_, &session_);
  // Intentionally no SetCheckpoint().
  controller.Start();
  controller.RequestHandover();

  ASSERT_TRUE(done.Wait());
  controller.Stop();

  EXPECT_EQ(State(), FuseUpgradeState::kFuseNormal);
}

// A stray/invalid SIGHUP (WaitHandoverPrepare returns false) must not touch the
// session at all -- keep serving.
TEST_F(HandoverControllerTest, InvalidPrepare_KeepsServing) {
  Latch done;
  EXPECT_CALL(peer_, WaitHandoverPrepare()).WillOnce(InvokeWithoutArgs([&] {
    done.Signal();
    return false;
  }));
  EXPECT_CALL(session_, PauseReceive()).Times(0);
  EXPECT_CALL(peer_, NotifyHandoverAbort()).Times(0);
  EXPECT_CALL(session_, Exit()).Times(0);

  HandoverController controller(Options(), &peer_, &session_);
  controller.SetCheckpoint([]() -> Status { return Status::OK(); });
  controller.Start();
  controller.RequestHandover();

  ASSERT_TRUE(done.Wait());
  controller.Stop();
}

// Past the checkpoint the VFS is torn down and cannot be resumed: the session
// must exit even when the READY notification cannot reach the new process.
TEST_F(HandoverControllerTest, CommitExitsEvenIfReadyNotifyFails) {
  Latch done;
  {
    InSequence seq;
    EXPECT_CALL(peer_, WaitHandoverPrepare()).WillOnce(Return(true));
    EXPECT_CALL(session_, PauseReceive());
    EXPECT_CALL(session_, WaitDrained(_)).WillOnce(Return(0));
    EXPECT_CALL(peer_, NotifyReadyToExit()).WillOnce(Return(false));
    EXPECT_CALL(session_, Exit()).WillOnce(InvokeWithoutArgs([&] {
      done.Signal();
    }));
  }
  EXPECT_CALL(session_, ResumeReceive()).Times(0);

  HandoverController controller(Options(), &peer_, &session_);
  controller.SetCheckpoint([]() -> Status { return Status::OK(); });
  controller.Start();
  controller.RequestHandover();

  ASSERT_TRUE(done.Wait());
  controller.Stop();
}

// Checkpoint runs but fails: abort (resume + state rollback), do not exit.
TEST_F(HandoverControllerTest, CheckpointFails_Aborts) {
  Latch done;
  {
    InSequence seq;
    EXPECT_CALL(peer_, WaitHandoverPrepare()).WillOnce(Return(true));
    EXPECT_CALL(session_, PauseReceive());
    EXPECT_CALL(session_, WaitDrained(_)).WillOnce(Return(0));
    EXPECT_CALL(peer_, NotifyHandoverAbort());
    EXPECT_CALL(session_, ResumeReceive()).WillOnce(InvokeWithoutArgs([&] {
      done.Signal();
    }));
  }
  EXPECT_CALL(session_, Exit()).Times(0);
  EXPECT_CALL(peer_, NotifyReadyToExit()).Times(0);

  HandoverController controller(Options(), &peer_, &session_);
  controller.SetCheckpoint(
      []() -> Status { return Status::Internal("checkpoint boom"); });
  controller.Start();
  controller.RequestHandover();

  ASSERT_TRUE(done.Wait());
  controller.Stop();

  EXPECT_EQ(State(), FuseUpgradeState::kFuseNormal);
}

// Lifecycle: RequestHandover before Start is rejected; Stop is idempotent.
TEST_F(HandoverControllerTest, LifecycleStartStopIdempotent) {
  HandoverController controller(Options(), &peer_, &session_);
  EXPECT_FALSE(controller.RequestHandover());  // not started yet
  controller.Stop();                           // no-op before start
  controller.Start();
  controller.Stop();
  controller.Stop();  // idempotent
}

}  // namespace fuse
}  // namespace client
}  // namespace dingofs
