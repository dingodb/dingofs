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
 * Created Date: 2026-11-23
 * Author: AI
 */

#include <gtest/gtest.h>

#include <cstdint>

#include "blockcache/utils/validator.h"

namespace dingofs {
namespace blockcache {

TEST(ValidatorTest, Positive) {
  EXPECT_FALSE(Positive("flag", 0));
  EXPECT_TRUE(Positive("flag", 1));
  EXPECT_TRUE(Positive("flag", UINT32_MAX));
}

}  // namespace blockcache
}  // namespace dingofs
