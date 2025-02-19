// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <cstdint>
#include <string>

#include "fmt/core.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "mdsv2/coordinator/dummy_coordinator_client.h"
#include "mdsv2/filesystem/id_generator.h"

namespace dingofs {
namespace mdsv2 {
namespace unit_test {

// test AutoIncrementIdGenerator
class AutoIncrementIdGeneratorTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() {}

  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(AutoIncrementIdGeneratorTest, GenID) {
  auto coordinator_client = DummyCoordinatorClient::New();
  ASSERT_TRUE(coordinator_client->Init("")) << "init coordinator client fail.";

  int64_t table_id = 1001;
  auto id_generator = AutoIncrementIdGenerator::New(coordinator_client, table_id, 20000, 8);
  ASSERT_TRUE(id_generator->Init()) << "init id generator fail.";

  for (int i = 0; i < 1000; ++i) {
    int64_t id = 0;
    ASSERT_TRUE(id_generator->GenID(id));
    ASSERT_EQ(id, 20000 + i);
  }
}

}  // namespace unit_test
}  // namespace mdsv2
}  // namespace dingofs