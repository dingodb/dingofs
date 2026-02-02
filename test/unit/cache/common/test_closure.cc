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

#include "cache/common/closure.h"

namespace dingofs {
namespace cache {

class ClosureTest : public ::testing::Test {};

class TestClosure : public Closure {
 public:
  void Run() override { run_called_ = true; }
  bool RunCalled() const { return run_called_; }

 private:
  bool run_called_{false};
};

TEST_F(ClosureTest, DefaultStatus) {
  TestClosure closure;
  EXPECT_TRUE(closure.status().ok());
}

TEST_F(ClosureTest, SetStatus) {
  TestClosure closure;
  closure.status() = Status::NotFound("not found");
  EXPECT_TRUE(closure.status().IsNotFound());
}

TEST_F(ClosureTest, ConstStatus) {
  TestClosure closure;
  closure.status() = Status::IoError("io error");

  const TestClosure& const_closure = closure;
  EXPECT_TRUE(const_closure.status().IsIoError());
}

TEST_F(ClosureTest, Run) {
  TestClosure closure;
  EXPECT_FALSE(closure.RunCalled());

  closure.Run();
  EXPECT_TRUE(closure.RunCalled());
}

}  // namespace cache
}  // namespace dingofs
