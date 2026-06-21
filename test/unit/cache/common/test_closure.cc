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
 * Created Date: 2026-06-21
 * Author: AI
 */

#include "cache/common/closure.h"

#include <gtest/gtest.h>

namespace dingofs {
namespace cache {

namespace {
// Closure leaves Run() abstract; a minimal concrete subclass lets us exercise
// the status() accessors and confirm the callback fires.
class FakeClosure : public Closure {
 public:
  void Run() override { ran = true; }
  bool ran = false;
};
}  // namespace

TEST(ClosureTest, StatusAccessor) {
  FakeClosure closure;
  EXPECT_TRUE(closure.status().ok());  // default-constructed Status is OK

  closure.status() = Status::IoError("disk failure");
  EXPECT_TRUE(closure.status().IsIoError());

  const Closure& const_ref = closure;
  EXPECT_TRUE(const_ref.status().IsIoError());
}

TEST(ClosureTest, Run) {
  FakeClosure closure;
  EXPECT_FALSE(closure.ran);
  closure.Run();
  EXPECT_TRUE(closure.ran);
}

}  // namespace cache
}  // namespace dingofs
