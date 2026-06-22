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
 * Created Date: 2026-06-21
 * Author: AI
 */

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <thread>

#include "cache/iutil/state_machine.h"
#include "cache/iutil/state_machine_impl.h"

namespace dingofs {
namespace cache {
namespace iutil {

namespace {
// State transitions are driven by an async execution queue, so observed state
// lags the triggering call. Poll until it settles (or time out).
bool WaitForState(StateMachine* sm, State expected, int timeout_ms = 3000) {
  for (int waited = 0; waited < timeout_ms; waited += 10) {
    if (sm->GetState() == expected) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return sm->GetState() == expected;
}
}  // namespace

class StateMachineTest : public ::testing::Test {};

TEST_F(StateMachineTest, StartAndShutdownIdempotent) {
  StateMachineImpl sm{std::make_unique<IConfiguration>()};
  EXPECT_EQ(sm.GetState(), kStateUnknown);

  EXPECT_TRUE(sm.Start());
  EXPECT_TRUE(sm.Start());  // already running
  EXPECT_EQ(sm.GetState(), kStateNormal);

  EXPECT_TRUE(sm.Shutdown());
  EXPECT_TRUE(sm.Shutdown());  // already down
}

TEST_F(StateMachineTest, SuccessAndErrorBeforeStartAreNoop) {
  StateMachineImpl sm{std::make_unique<IConfiguration>()};
  sm.Error(100);
  sm.Success(100);
  EXPECT_EQ(sm.GetState(), kStateUnknown);
}

TEST_F(StateMachineTest, NormalToUnstableOnErrors) {
  StateMachineImpl sm{std::make_unique<IConfiguration>()};
  ASSERT_TRUE(sm.Start());
  ASSERT_EQ(sm.GetState(), kStateNormal);

  // default normal2unstable_error_num() == 3, so 4 errors trip the transition
  sm.Error(4);
  EXPECT_TRUE(WaitForState(&sm, kStateUnstable));

  ASSERT_TRUE(sm.Shutdown());
}

TEST_F(StateMachineTest, UnstableToNormalOnSuccesses) {
  StateMachineImpl sm{std::make_unique<IConfiguration>()};
  ASSERT_TRUE(sm.Start());

  sm.Error(4);
  ASSERT_TRUE(WaitForState(&sm, kStateUnstable));

  // default unstable2normal_succ_num() == 10, so 11 successes return to normal
  sm.Success(11);
  EXPECT_TRUE(WaitForState(&sm, kStateNormal));

  ASSERT_TRUE(sm.Shutdown());
}

TEST_F(StateMachineTest, EventDrivenTransitions) {
  StateMachineImpl sm{std::make_unique<IConfiguration>()};
  ASSERT_TRUE(sm.Start());

  {  // Normal only reacts to the Unstable event
    sm.OnEvent(kStateEventNormal);
    sm.OnEvent(kStateEventDown);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(sm.GetState(), kStateNormal);
  }

  {  // Normal -> Unstable
    sm.OnEvent(kStateEventUnstable);
    EXPECT_TRUE(WaitForState(&sm, kStateUnstable));
  }

  {  // Unstable -> Down
    sm.OnEvent(kStateEventDown);
    EXPECT_TRUE(WaitForState(&sm, kStateDown));
  }

  {  // Down is terminal
    sm.OnEvent(kStateEventNormal);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(sm.GetState(), kStateDown);
  }

  ASSERT_TRUE(sm.Shutdown());
}

TEST_F(StateMachineTest, HonorsCustomConfiguration) {
  // Regression: the ctor used to take IConfiguration by value, slicing any
  // derived Configure so its overridden thresholds were ignored. With a custom
  // threshold of 1, two errors must trip Normal->Unstable (default is 3).
  struct FastConfig : public IConfiguration {
    int normal2unstable_error_num() override { return 1; }
  };

  StateMachineImpl sm{std::make_unique<FastConfig>()};
  ASSERT_TRUE(sm.Start());
  ASSERT_EQ(sm.GetState(), kStateNormal);

  sm.Error(2);  // 2 > 1 honors the custom threshold (not the default 3)
  EXPECT_TRUE(WaitForState(&sm, kStateUnstable));

  ASSERT_TRUE(sm.Shutdown());
}

TEST_F(StateMachineTest, StateAndEventToString) {
  EXPECT_EQ(StateToString(kStateUnknown), "unknown");
  EXPECT_EQ(StateToString(kStateNormal), "normal");
  EXPECT_EQ(StateToString(kStateUnstable), "unstable");
  EXPECT_EQ(StateToString(kStateDown), "down");

  EXPECT_EQ(StateEventToString(kStateEventUnkown), "StateEventUnkown");
  EXPECT_EQ(StateEventToString(kStateEventNormal), "StateEventNormal");
  EXPECT_EQ(StateEventToString(kStateEventUnstable), "StateEventUnstable");
  EXPECT_EQ(StateEventToString(kStateEventDown), "StateEventDown");
}

}  // namespace iutil
}  // namespace cache
}  // namespace dingofs
