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

#include <gtest/gtest.h>

#include <string>

#include "cache/iutil/state_machine.h"

namespace dingofs {
namespace cache {
namespace iutil {

class StateMachineTest : public ::testing::Test {};

TEST_F(StateMachineTest, StateToString) {
  EXPECT_EQ(StateToString(kStateUnknown), "unknown");
  EXPECT_EQ(StateToString(kStateNormal), "normal");
  EXPECT_EQ(StateToString(kStateUnStable), "unstable");
  EXPECT_EQ(StateToString(kStateDown), "down");
}

TEST_F(StateMachineTest, StateEventToString) {
  EXPECT_EQ(StateEventToString(kStateEventUnkown), "StateEventUnkown");
  EXPECT_EQ(StateEventToString(kStateEventNormal), "StateEventNormal");
  EXPECT_EQ(StateEventToString(kStateEventUnstable), "StateEventUnstable");
  EXPECT_EQ(StateEventToString(kStateEventDown), "StateEventDown");
}

TEST_F(StateMachineTest, IConfigurationDefaults) {
  IConfiguration config;
  EXPECT_EQ(config.tick_duration_s(), 60);
  EXPECT_EQ(config.normal2unstable_error_num(), 3);
  EXPECT_EQ(config.unstable2normal_succ_num(), 10);
  EXPECT_EQ(config.unstable2down_s(), 1800);
}

}  // namespace iutil
}  // namespace cache
}  // namespace dingofs
