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
 * Created Date: 2026-11-23
 * Author: AI
 */

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <thread>
#include <vector>

#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/core/reactor/poller.h"
#include "blockcache/core/reactor/reactor.h"
#include "test/unit/blockcache/base/runtime.h"

namespace dingofs {
namespace blockcache {

class NotingPoller final : public Poller {
 public:
  NotingPoller(int id, std::vector<int>* polled) : id_(id), polled_(polled) {}

  bool Poll() override {
    polled_->push_back(id_);
    return false;
  }
  bool PurePoll() override { return false; }

 private:
  const int id_;
  std::vector<int>* polled_;
};

TEST(PollerRegistrationTest, Register) {
  ScopedRuntime runtime;

  RunOnShard(0, []() -> Future<> {
    std::vector<int> polled;
    NotingPoller first(1, &polled);
    NotingPoller second(2, &polled);

    {
      const PollerRegistration first_registration(ThisReactor(), &first);
      const PollerRegistration second_registration(ThisReactor(), &second);
      EXPECT_TRUE(
          co_await YieldUntil([&polled] { return polled.size() >= 2; }));
      if (polled.size() >= 2) {
        EXPECT_EQ(polled[0], 1);
        EXPECT_EQ(polled[1], 2);
      }
    }

    {
      polled.clear();
      EXPECT_FALSE(co_await YieldUntil([&polled] { return !polled.empty(); },
                                       std::chrono::milliseconds(50)));
    }
  });
}

TEST(PollerRegistrationTest, OffItsReactorsThread) {
  const auto register_off_thread = [] {
    Reactor reactor(0);
    std::vector<int> polled;
    NotingPoller poller(1, &polled);
    std::thread([&reactor, &poller] {
      const PollerRegistration registration(reactor, &poller);
    }).join();
  };
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  EXPECT_DEATH(register_off_thread(),
               "PollerRegistration built off its reactor's thread");
}

}  // namespace blockcache
}  // namespace dingofs
