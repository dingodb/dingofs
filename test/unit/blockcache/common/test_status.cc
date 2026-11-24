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
 * Created Date: 2026-09-24
 * Author: AI
 */

#include <gtest/gtest.h>

#include <cerrno>
#include <memory>
#include <string>
#include <utility>

#include "blockcache/common/status.h"
#include "common/status.h"

namespace dingofs {
namespace blockcache {

TEST(StatusOrTest, FromError) {
  StatusOr<std::string> result(Status::NotFound(ENOENT, "no such block"));
  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsNotFound());
  EXPECT_EQ(result.status().Errno(), ENOENT);
  EXPECT_EQ(result.status().ToString(), "NotFound (errno:2) : no such block");

  {  // as a return statement builds it
    auto make = []() -> StatusOr<int> {
      return Status::CacheFull("cache full");
    };
    EXPECT_TRUE(make().status().IsCacheFull());
  }
}

TEST(StatusOrTest, FromValue) {
  {  // a copyable value
    StatusOr<std::string> result(std::string("hello dingofs"));
    ASSERT_TRUE(result.ok());
    EXPECT_TRUE(result.status().ok());
    EXPECT_EQ(result.value(), "hello dingofs");
    EXPECT_EQ(*result, "hello dingofs");
    EXPECT_EQ(result->size(), 13u);

    result.value().append("!");  // the value itself, not a copy
    EXPECT_EQ(*result, "hello dingofs!");
  }

  {  // a move-only one
    StatusOr<std::unique_ptr<int>> result(std::make_unique<int>(42));
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(**result, 42);
    EXPECT_EQ(*result->get(), 42);
  }

  {  // as a return statement builds it
    auto make = []() -> StatusOr<int> { return 7; };
    EXPECT_EQ(*make(), 7);
  }
}

TEST(StatusOrTest, Move) {
  using Shared = std::shared_ptr<int>;
  const Shared counted = std::make_shared<int>(1);  // use_count(): who holds it

  {  // the move constructor takes the value along and leaves none behind
    StatusOr<Shared> source(counted);
    EXPECT_EQ(counted.use_count(), 2);

    StatusOr<Shared> moved(std::move(source));
    EXPECT_FALSE(source.ok());  // NOLINT(bugprone-use-after-move)
    ASSERT_TRUE(moved.ok());
    EXPECT_EQ(*moved, counted);
    EXPECT_EQ(counted.use_count(), 2);
  }
  EXPECT_EQ(counted.use_count(), 1);  // released with the StatusOr

  {  // an error moves too
    StatusOr<Shared> source(Status::IoError(EIO, "read failed"));
    StatusOr<Shared> moved(std::move(source));
    EXPECT_FALSE(moved.ok());
    EXPECT_TRUE(moved.status().IsIoError());
    EXPECT_EQ(moved.status().Errno(), EIO);
    EXPECT_EQ(moved.status().ToString(), "IoError (errno:5) : read failed");
  }

  {  // move assignment: whatever was held before is released first
    const Shared other = std::make_shared<int>(2);
    StatusOr<Shared> target(other);
    StatusOr<Shared> source(counted);

    target = std::move(source);  // a value over a value
    EXPECT_EQ(other.use_count(), 1);
    EXPECT_EQ(counted.use_count(), 2);
    ASSERT_TRUE(target.ok());
    EXPECT_EQ(*target, counted);
    EXPECT_FALSE(source.ok());  // NOLINT(bugprone-use-after-move)

    target = StatusOr<Shared>(Status::Abort("aborted"));  // an error over it
    EXPECT_FALSE(target.ok());
    EXPECT_TRUE(target.status().IsAbort());
    EXPECT_EQ(counted.use_count(), 1);

    target = StatusOr<Shared>(counted);  // a value over an error
    ASSERT_TRUE(target.ok());
    EXPECT_EQ(counted.use_count(), 2);

    StatusOr<Shared>& self = target;  // onto itself: nothing happens
    target = std::move(self);
    ASSERT_TRUE(target.ok());
    EXPECT_EQ(*target, counted);
    EXPECT_EQ(counted.use_count(), 2);
  }
  EXPECT_EQ(counted.use_count(), 1);

  {  // value() and * on an rvalue hand the value over
    StatusOr<std::unique_ptr<int>> result(std::make_unique<int>(42));
    const std::unique_ptr<int> taken = std::move(result).value();
    EXPECT_EQ(*taken, 42);
    ASSERT_TRUE(result.ok());  // NOLINT(bugprone-use-after-move)
    EXPECT_EQ(result->get(), nullptr);

    StatusOr<std::unique_ptr<int>> again(std::make_unique<int>(43));
    const std::unique_ptr<int> taken_too = *std::move(again);
    EXPECT_EQ(*taken_too, 43);
  }
}

}  // namespace blockcache
}  // namespace dingofs
