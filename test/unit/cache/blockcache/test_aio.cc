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
 * Created Date: 2026-02-25
 * Author: AI
 */

#include <gtest/gtest.h>

#include <chrono>
#include <iostream>
#include <sstream>
#include <thread>

#include "cache/blockcache/aio.h"

namespace dingofs {
namespace cache {

class AioTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ctx_ = std::make_shared<Context>();
    buffer_ = new char[4096];
  }

  void TearDown() override { delete[] buffer_; }

  ContextSPtr ctx_;
  char* buffer_;
};

TEST_F(AioTest, Constructor) {
  {
    Aio aio(ctx_, 10, 0, 4096, buffer_, 0, true);

    const auto& attr = aio.Attr();
    EXPECT_EQ(attr.ctx, ctx_);
    EXPECT_EQ(attr.fd, 10);
    EXPECT_EQ(attr.offset, 0);
    EXPECT_EQ(attr.length, 4096);
    EXPECT_EQ(attr.buffer, buffer_);
    EXPECT_EQ(attr.buf_index, 0);
    EXPECT_TRUE(attr.for_read);

    const auto& result = aio.Result();
    EXPECT_TRUE(result.status.IsUnknown());
  }

  {
    Aio aio(ctx_, 5, 1024, 2048, buffer_, 1, false);

    const auto& attr = aio.Attr();
    EXPECT_EQ(attr.ctx, ctx_);
    EXPECT_EQ(attr.fd, 5);
    EXPECT_EQ(attr.offset, 1024);
    EXPECT_EQ(attr.length, 2048);
    EXPECT_EQ(attr.buf_index, 1);
    EXPECT_FALSE(attr.for_read);
  }

  {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(Aio(ctx_, -1, 0, 4096, buffer_, 0, true), "");
    EXPECT_DEATH(Aio(ctx_, 10, -1, 4096, buffer_, 0, true), "");
    EXPECT_DEATH(Aio(ctx_, 10, 0, 0, buffer_, 0, true), "");
    EXPECT_DEATH(Aio(ctx_, 10, 0, 4096, nullptr, 0, true), "");
  }
}

TEST_F(AioTest, AttrAndResult) {
  Aio aio(ctx_, 10, 0, 4096, buffer_, 0, true);

  {
    const auto& attr = aio.Attr();
    EXPECT_EQ(attr.fd, 10);
    EXPECT_EQ(attr.length, 4096);
  }

  {
    const auto& result = aio.Result();
    EXPECT_TRUE(result.status.IsUnknown());
  }

  {
    aio.Result().status = Status::OK();
    EXPECT_TRUE(aio.Result().status.ok());
  }

  {
    aio.Result().status = Status::IoError("test error");
    EXPECT_TRUE(aio.Result().status.IsIoError());
  }
}

TEST_F(AioTest, WaitAndRun) {
  {
    Aio aio(ctx_, 10, 0, 4096, buffer_, 0, true);

    std::thread worker([&aio]() {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      aio.Result().status = Status::OK();
      aio.Run();
    });

    aio.Wait();

    EXPECT_TRUE(aio.Result().status.ok());

    worker.join();
  }

  {
    Aio aio(ctx_, 10, 0, 4096, buffer_, 0, true);

    auto start = std::chrono::steady_clock::now();

    std::thread worker([&aio]() {
      std::this_thread::sleep_for(std::chrono::seconds(3));
      aio.Result().status = Status::OK();
      aio.Run();
    });

    aio.Wait();

    auto end = std::chrono::steady_clock::now();
    auto elapsed =
        std::chrono::duration_cast<std::chrono::seconds>(end - start).count();

    EXPECT_GE(elapsed, 3);
    EXPECT_TRUE(aio.Result().status.ok());

    worker.join();
  }
}

TEST_F(AioTest, OutputStream) {
  {
    Aio aio(ctx_, 10, 1024, 4096, buffer_, 2, true);
    std::ostringstream oss;
    oss << aio;

    std::string output = oss.str();
    EXPECT_NE(output.find("Aio{"), std::string::npos);
    EXPECT_NE(output.find("op=read"), std::string::npos);
    EXPECT_NE(output.find("fd=10"), std::string::npos);
    EXPECT_NE(output.find("offset=1024"), std::string::npos);
    EXPECT_NE(output.find("length=4096"), std::string::npos);
    EXPECT_NE(output.find("buffer=0x"), std::string::npos);
    EXPECT_NE(output.find("buf_index=2"), std::string::npos);
  }

  {
    Aio aio(ctx_, 5, 0, 2048, buffer_, 1, false);
    std::ostringstream oss;
    oss << aio;

    std::string output = oss.str();
    EXPECT_NE(output.find("op=write"), std::string::npos);
    EXPECT_NE(output.find("fd=5"), std::string::npos);
  }

  {
    Aio aio(ctx_, 10, 1024, 4096, buffer_, 2, true);
    std::cout << "Aio example output: " << aio << "\n";
  }
}

}  // namespace cache
}  // namespace dingofs
