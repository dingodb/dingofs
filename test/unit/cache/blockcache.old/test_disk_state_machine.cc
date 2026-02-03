// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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

#include <unistd.h>

#include "cache/blockcache/disk_state_machine_impl.h"
#include "cache/common/common.h"
#include "gtest/gtest.h"

namespace dingofs {
namespace cache {

class DiskStateMachineTest : public ::testing::Test {
 public:
  DiskStateMachineTest() = default;

  ~DiskStateMachineTest() override = default;
};

TEST_F(DiskStateMachineTest, Init) {
  DiskStateMachineImpl disk_state_machine(nullptr);

  EXPECT_EQ(disk_state_machine.GetDiskState(), DiskState::kDiskStateNormal);

  EXPECT_TRUE(disk_state_machine.Start());

  EXPECT_TRUE(disk_state_machine.Stop());
}

TEST_F(DiskStateMachineTest, Normal2Unstable) {
  DiskStateMachineImpl disk_state_machine(nullptr);

  EXPECT_TRUE(disk_state_machine.Start());

  for (auto i = 0; i <= FLAGS_disk_state_normal2unstable_io_error_num; i++) {
    disk_state_machine.IOErr();
  }
  sleep(1);

  EXPECT_EQ(disk_state_machine.GetDiskState(), DiskState::kDiskStateUnStable);

  EXPECT_TRUE(disk_state_machine.Stop());
}

TEST_F(DiskStateMachineTest, Unstable2Down) {
  DiskStateMachineImpl disk_state_machine(nullptr);

  // NOTE: must set before start
  FLAGS_disk_state_tick_duration_s = 5;

  EXPECT_TRUE(disk_state_machine.Start());

  for (auto i = 0; i <= FLAGS_disk_state_normal2unstable_io_error_num; i++) {
    disk_state_machine.IOErr();
  }
  sleep(1);

  EXPECT_EQ(disk_state_machine.GetDiskState(), DiskState::kDiskStateUnStable);

  FLAGS_disk_state_unstable2down_s = 10;
  sleep(20);

  EXPECT_EQ(disk_state_machine.GetDiskState(), DiskState::kDiskStateDown);

  EXPECT_TRUE(disk_state_machine.Stop());
}

TEST_F(DiskStateMachineTest, Unstable2Normal) {
  DiskStateMachineImpl disk_state_machine(nullptr);

  EXPECT_TRUE(disk_state_machine.Start());

  {
    for (auto i = 0; i <= FLAGS_disk_state_normal2unstable_io_error_num; i++) {
      disk_state_machine.IOErr();
    }
    sleep(1);
    EXPECT_EQ(disk_state_machine.GetDiskState(), DiskState::kDiskStateUnStable);
  }

  {
    for (auto i = 0; i <= FLAGS_disk_state_unstable2normal_io_succ_num; i++) {
      disk_state_machine.IOSucc();
    }
    sleep(1);
    EXPECT_EQ(disk_state_machine.GetDiskState(), DiskState::kDiskStateNormal);
  }

  EXPECT_TRUE(disk_state_machine.Stop());
}

}  // namespace cache
}  // namespace dingofs
