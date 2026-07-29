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

#include <cstdint>
#include <limits>

#include "common/file_size.h"

namespace dingofs {

TEST(FileSizeTest, ComputesMaxFileSize) {
  constexpr uint64_t kChunkSize = uint64_t{64} * 1024 * 1024;
  uint64_t max_file_size = 0;

  ASSERT_TRUE(TryGetMaxFileSize(kChunkSize, &max_file_size));
  EXPECT_EQ(max_file_size, uint64_t{1} << 57);
  EXPECT_FALSE(TryGetMaxFileSize(0, &max_file_size));
  EXPECT_FALSE(TryGetMaxFileSize(kChunkSize, nullptr));
}

TEST(FileSizeTest, ValidatesExactSizeBoundary) {
  constexpr uint64_t kMax = uint64_t{1} << 57;

  EXPECT_TRUE(IsValidFileSize(kMax, kMax));
  EXPECT_FALSE(IsValidFileSize(kMax + 1, kMax));
}

TEST(FileSizeTest, ValidatesRangesWithoutOverflow) {
  constexpr uint64_t kMax = uint64_t{1} << 57;

  EXPECT_TRUE(IsValidFileRange(kMax - 1, 1, kMax));
  EXPECT_FALSE(IsValidFileRange(kMax - 1, 2, kMax));
  EXPECT_FALSE(IsValidFileRange(kMax, 1, kMax));
  EXPECT_FALSE(IsValidFileRange(std::numeric_limits<uint64_t>::max(), 1, kMax));
  EXPECT_TRUE(IsValidFileRange(std::numeric_limits<uint64_t>::max(), 0, kMax));
}

}  // namespace dingofs
