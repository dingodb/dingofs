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

#include <gtest/gtest.h>

#include "cache/remotecache/peer_health_checker.h"

namespace dingofs {
namespace cache {

class PeerHealthCheckerTest : public ::testing::Test {};

TEST_F(PeerHealthCheckerTest, Construction) {
  PeerHealthChecker checker("127.0.0.1", 8080);
  EXPECT_TRUE(checker.IsHealthy());
}

TEST_F(PeerHealthCheckerTest, IOSuccessIncrement) {
  PeerHealthChecker checker("127.0.0.1", 8080);

  checker.IOSuccess();
  checker.IOSuccess();
  checker.IOSuccess();

  EXPECT_TRUE(checker.IsHealthy());
}

TEST_F(PeerHealthCheckerTest, IOErrorIncrement) {
  PeerHealthChecker checker("127.0.0.1", 8080);

  checker.IOError();
  checker.IOError();

  // Just calling IOError should not immediately make it unhealthy
  EXPECT_TRUE(checker.IsHealthy());
}

TEST_F(PeerHealthCheckerTest, MixedIOOperations) {
  PeerHealthChecker checker("127.0.0.1", 8080);

  for (int i = 0; i < 100; ++i) {
    checker.IOSuccess();
  }

  for (int i = 0; i < 5; ++i) {
    checker.IOError();
  }

  // Still healthy with mostly successes
  EXPECT_TRUE(checker.IsHealthy());
}

TEST_F(PeerHealthCheckerTest, DifferentEndpoints) {
  PeerHealthChecker checker1("192.168.1.1", 8080);
  PeerHealthChecker checker2("192.168.1.2", 8081);

  EXPECT_TRUE(checker1.IsHealthy());
  EXPECT_TRUE(checker2.IsHealthy());

  checker1.IOError();
  checker1.IOError();
  checker1.IOError();

  // Checker1's state doesn't affect checker2
  EXPECT_TRUE(checker2.IsHealthy());
}

}  // namespace cache
}  // namespace dingofs
