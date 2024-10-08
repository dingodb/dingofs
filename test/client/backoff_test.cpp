/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * File Created: Statday, 26th Oct 2019 3:24:40 pm
 * Author: tongguangxun
 */

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "src/client/chunk_closure.h"
#include "src/client/config_info.h"

namespace curve {
namespace client {

TEST(ClientClosure, OverLoadBackOffTest) {
  FailureRequestOption failopt;
  failopt.chunkserverMaxRetrySleepIntervalUS = 8000000;
  failopt.chunkserverOPRetryIntervalUS = 500000;

  ClientClosure::SetFailureRequestOption(failopt);

  for (int i = 1; i < 1000; i++) {
    if (i < ClientClosure::backoffParam_.maxOverloadPow) {
      uint64_t curTime = failopt.chunkserverOPRetryIntervalUS * std::pow(2, i);
      ASSERT_LE(ClientClosure::OverLoadBackOff(i), curTime + 0.1 * curTime);
      ASSERT_GE(ClientClosure::OverLoadBackOff(i), curTime - 0.1 * curTime);
    } else {
      ASSERT_LE(ClientClosure::OverLoadBackOff(i),
                failopt.chunkserverMaxRetrySleepIntervalUS +
                    0.1 * failopt.chunkserverMaxRetrySleepIntervalUS);
      ASSERT_GE(ClientClosure::OverLoadBackOff(i),
                failopt.chunkserverMaxRetrySleepIntervalUS -
                    0.1 * failopt.chunkserverMaxRetrySleepIntervalUS);
    }
  }

  failopt.chunkserverMaxRetrySleepIntervalUS = 64000000;
  failopt.chunkserverOPRetryIntervalUS = 500000;

  ClientClosure::SetFailureRequestOption(failopt);

  for (int i = 1; i < 1000; i++) {
    if (i < ClientClosure::backoffParam_.maxOverloadPow) {
      uint64_t curTime = failopt.chunkserverOPRetryIntervalUS * std::pow(2, i);
      ASSERT_LE(ClientClosure::OverLoadBackOff(i), curTime + 0.1 * curTime);
      ASSERT_GE(ClientClosure::OverLoadBackOff(i), curTime - 0.1 * curTime);
    } else {
      ASSERT_LE(ClientClosure::OverLoadBackOff(i),
                failopt.chunkserverMaxRetrySleepIntervalUS +
                    0.1 * failopt.chunkserverMaxRetrySleepIntervalUS);
      ASSERT_GE(ClientClosure::OverLoadBackOff(i),
                failopt.chunkserverMaxRetrySleepIntervalUS -
                    0.1 * failopt.chunkserverMaxRetrySleepIntervalUS);
    }
  }
}

TEST(ClientClosure, TimeoutBackOffTest) {
  FailureRequestOption failopt;
  failopt.chunkserverMaxRPCTimeoutMS = 3000;
  failopt.chunkserverRPCTimeoutMS = 500;

  ClientClosure::SetFailureRequestOption(failopt);

  for (int i = 1; i < 1000; i++) {
    if (i < ClientClosure::backoffParam_.maxTimeoutPow) {
      uint64_t curTime = failopt.chunkserverRPCTimeoutMS * std::pow(2, i);
      ASSERT_EQ(ClientClosure::TimeoutBackOff(i), curTime);
    } else {
      ASSERT_EQ(ClientClosure::TimeoutBackOff(i), 2000);
    }
  }

  failopt.chunkserverMaxRPCTimeoutMS = 4000;
  failopt.chunkserverRPCTimeoutMS = 500;

  ClientClosure::SetFailureRequestOption(failopt);

  for (int i = 1; i < 1000; i++) {
    if (i < ClientClosure::backoffParam_.maxTimeoutPow) {
      uint64_t curTime = failopt.chunkserverRPCTimeoutMS * std::pow(2, i);
      ASSERT_EQ(ClientClosure::TimeoutBackOff(i), curTime);
    } else {
      ASSERT_EQ(ClientClosure::TimeoutBackOff(i), 4000);
    }
  }
}

}  // namespace client
}  // namespace curve
