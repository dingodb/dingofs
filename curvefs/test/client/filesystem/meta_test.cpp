

/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Project: Curve
 * Created Date: 2023-04-03
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/src/client/filesystem/meta.h"

#include <gtest/gtest.h>

#include <sstream>

namespace curvefs {
namespace client {
namespace filesystem {

class HandlerManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(HandlerManagerTest, Basic) {
  // CASE 1: new handler
  auto manager = std::make_shared<HandlerManager>();
  for (auto i = 0; i < 10; i++) {
    auto handler = manager->NewHandler();
    ASSERT_EQ(handler->fh, i);
    ASSERT_FALSE(handler->padding);
    ASSERT_NE(handler->buffer, nullptr);
  }

  // CASE 2: find handler
  ASSERT_EQ(manager->FindHandler(10), nullptr);
  for (auto i = 0; i < 10; i++) {
    auto handler = manager->FindHandler(i);
    ASSERT_NE(handler, nullptr);
    ASSERT_EQ(handler->fh, i);
  }

  // CASE 3: release handler
  for (auto i = 0; i < 10; i++) {
    manager->ReleaseHandler(i);
    auto handler = manager->FindHandler(i);
    ASSERT_EQ(handler, nullptr);
  }
}

TEST_F(HandlerManagerTest, ModfidyHandler) {
  auto manager = std::make_shared<HandlerManager>();
  manager->NewHandler();  // fh = 0

  auto handler = manager->FindHandler(0);
  ASSERT_NE(handler, nullptr);
  ASSERT_FALSE(handler->padding);
  ASSERT_NE(handler->buffer, nullptr);

  auto buffer = handler->buffer;
  buffer->size = 10;
  buffer->p = static_cast<char*>(realloc(buffer->p, buffer->size));
  handler->padding = true;
  char* position = buffer->p;

  handler = manager->FindHandler(0);
  buffer = handler->buffer;
  ASSERT_EQ(buffer->size, 10);
  ASSERT_EQ(buffer->p, position);
  ASSERT_TRUE(handler->padding);
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs
