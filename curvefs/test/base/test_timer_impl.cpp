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

#include <memory>

#include "curvefs/src/base/timer/timer_impl.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

namespace curvefs {

namespace base {
namespace timer {

class TimerImplTest : public ::testing::Test {
 public:
  TimerImplTest() = default;

  ~TimerImplTest() override = default;
};

TEST_F(TimerImplTest, BaseTest) {
  auto timer = std::make_unique<TimerImpl>();
  EXPECT_TRUE(timer->Start());

  EXPECT_TRUE(timer->Stop());

  EXPECT_FALSE(timer->Stop());
}

TEST_F(TimerImplTest, BGThreadNum) {
  int bg_num = 2;
  auto timer = std::make_unique<TimerImpl>(bg_num);
  EXPECT_EQ(timer->GetBgBthreadNum(), bg_num);

  timer = std::make_unique<TimerImpl>();
  EXPECT_EQ(timer->GetBgBthreadNum(), FLAGS_timer_bg_bthread_num);
}

TEST_F(TimerImplTest, Add) {
  auto timer = std::make_unique<TimerImpl>();

  EXPECT_TRUE(timer->Start());

  std::mutex mutex;
  std::condition_variable cond;
  std::atomic<int> count(2);

  timer->Add(
      [&]() {
        EXPECT_EQ(count.fetch_sub(1), 2);
        cond.notify_all();
      },
      5);

  timer->Add(
      [&]() {
        EXPECT_EQ(count.fetch_sub(1), 1);
        cond.notify_all();
      },
      10);

  {
    std::unique_lock<std::mutex> lg(mutex);
    while (count.load() != 0) {
      LOG(INFO) << "wait 1 ms";
      cond.wait_for(lg, std::chrono::milliseconds(1));
    }
  }

  EXPECT_EQ(count.load(), 0);
}

}  // namespace timer
}  // namespace base
}  // namespace curvefs
