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

#include <gtest/gtest.h>

#include <cstring>
#include <string>

#include "cache/iutil/string_util.h"

namespace dingofs {
namespace cache {
namespace iutil {

TEST(StringUtilTest, StringValidator) {
  EXPECT_FALSE(StringValidator("flag", ""));
  EXPECT_TRUE(StringValidator("flag", "x"));
  EXPECT_TRUE(StringValidator("flag", "some-value"));
  EXPECT_TRUE(StringValidator(nullptr, " "));  // whitespace is non-empty
}

TEST(StringUtilTest, DeleteBuffer) {
  // DeleteBuffer must release memory allocated with new[]; exercise it so leak
  // sanitizers can catch a mismatch. No observable return value to assert.
  char* buffer = new char[4096];
  std::memset(buffer, 'x', 4096);
  DeleteBuffer(buffer);

  // Usable as a void(*)(void*) deleter callback.
  void (*deleter)(void*) = &DeleteBuffer;
  deleter(new char[1]);
}

}  // namespace iutil
}  // namespace cache
}  // namespace dingofs
