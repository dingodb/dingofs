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

#include <sstream>
#include <thread>

#include "cache/blockcache/aio.h"

namespace dingofs {
namespace cache {

class AioTest : public ::testing::Test {};

TEST_F(AioTest, Constructor) {
  auto ctx = NewContext();
  char buffer[1024];
  Aio aio(ctx, 1, 0, 100, buffer, 0, true);

  EXPECT_EQ(aio.fd, 1);
  EXPECT_EQ(aio.offset, 0);
  EXPECT_EQ(aio.length, 100);
  EXPECT_EQ(aio.buffer, buffer);
  EXPECT_EQ(aio.buf_index, 0);
  EXPECT_TRUE(aio.for_read);
  EXPECT_FALSE(aio.finish);
}

TEST_F(AioTest, ConstructorWrite) {
  auto ctx = NewContext();
  char buffer[1024];
  Aio aio(ctx, 2, 100, 200, buffer, 1, false);

  EXPECT_EQ(aio.fd, 2);
  EXPECT_EQ(aio.offset, 100);
  EXPECT_EQ(aio.length, 200);
  EXPECT_EQ(aio.buf_index, 1);
  EXPECT_FALSE(aio.for_read);
}

TEST_F(AioTest, RunAndWait) {
  auto ctx = NewContext();
  char buffer[1024];
  Aio aio(ctx, 1, 0, 100, buffer, 0, true);

  // Run in a separate thread
  std::thread t([&aio]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    aio.Run();
  });

  aio.Wait();
  EXPECT_TRUE(aio.finish);

  t.join();
}

TEST_F(AioTest, Status) {
  auto ctx = NewContext();
  char buffer[1024];
  Aio aio(ctx, 1, 0, 100, buffer, 0, true);

  // Default status should be OK
  EXPECT_TRUE(aio.status().ok());

  // Set error status
  aio.status() = Status::IoError("test error");
  EXPECT_TRUE(aio.status().IsIoError());
}

TEST_F(AioTest, StreamOperatorRead) {
  auto ctx = NewContext();
  char buffer[1024];
  Aio aio(ctx, 5, 100, 200, buffer, 1, true);

  std::ostringstream oss;
  oss << aio;

  EXPECT_NE(oss.str().find("read"), std::string::npos);
  EXPECT_NE(oss.str().find("fd=5"), std::string::npos);
  EXPECT_NE(oss.str().find("offset=100"), std::string::npos);
  EXPECT_NE(oss.str().find("length=200"), std::string::npos);
}

TEST_F(AioTest, StreamOperatorWrite) {
  auto ctx = NewContext();
  char buffer[1024];
  Aio aio(ctx, 3, 50, 150, buffer, 2, false);

  std::ostringstream oss;
  oss << aio;

  EXPECT_NE(oss.str().find("write"), std::string::npos);
  EXPECT_NE(oss.str().find("fd=3"), std::string::npos);
}

}  // namespace cache
}  // namespace dingofs
