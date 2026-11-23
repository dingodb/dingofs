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
 * Created Date: 2026-10-04
 * Author: AI
 */

#include <gtest/gtest.h>

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <utility>
#include <vector>

#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/utils/sync.h"
#include "common/status.h"
#include "test/unit/blockcache/base/runtime.h"

namespace dingofs {
namespace blockcache {

struct Connect {
  int* runs;
  uint64_t delay_ms;
  Status status;

  Future<Status> operator()() const {
    ++*runs;
    co_await Sleep(std::chrono::milliseconds(delay_ms));
    co_return status;
  }
};

class SingleFlightTest : public ::testing::Test {
 protected:
  ScopedRuntime runtime_;
};

TEST_F(SingleFlightTest, Do) {
  {  // alone it runs the work and hands back its status
    const Status status = RunOnShard(0, []() -> Future<Status> {
      SingleFlight flight;
      int runs = 0;
      const Status status = co_await flight.Do(
          Connect{.runs = &runs, .delay_ms = 0, .status = Status::OK()});
      EXPECT_EQ(runs, 1);
      EXPECT_FALSE(flight.running());
      co_return status;
    });
    EXPECT_TRUE(status.ok()) << status.ToString();
  }

  {  // callers that come while it runs join that run and share its outcome
    RunOnShard(0, []() -> Future<> {
      SingleFlight flight;
      int runs = 0;
      const Connect connect{
          .runs = &runs,
          .delay_ms = 50,
          .status = Status::NetError(ECONNREFUSED, "connect to 10.0.0.2:9301")};

      std::vector<Future<Status>> callers;
      callers.push_back(flight.Do(connect));
      EXPECT_TRUE(flight.running());
      callers.push_back(flight.Do(connect));
      callers.push_back(flight.Do(connect));
      for (Future<Status>& caller : callers) {
        const Status status = co_await std::move(caller);
        EXPECT_TRUE(status.IsNetError()) << status.ToString();
        EXPECT_EQ(status.Errno(), ECONNREFUSED);
      }
      EXPECT_EQ(runs, 1);
      EXPECT_FALSE(flight.running());

      {  // the next caller after that starts a run of its own
        const Status status = co_await flight.Do(
            Connect{.runs = &runs, .delay_ms = 0, .status = Status::OK()});
        EXPECT_TRUE(status.ok()) << status.ToString();
        EXPECT_EQ(runs, 2);
      }
    });
  }
}

TEST_F(SingleFlightTest, Running) {
  RunOnShard(0, []() -> Future<> {
    SingleFlight flight;
    int runs = 0;
    EXPECT_FALSE(flight.running());
    Future<Status> probe = flight.Do(
        Connect{.runs = &runs, .delay_ms = 20, .status = Status::OK()});
    EXPECT_TRUE(flight.running());
    EXPECT_TRUE((co_await std::move(probe)).ok());
    EXPECT_FALSE(flight.running());
  });
}

}  // namespace blockcache
}  // namespace dingofs
