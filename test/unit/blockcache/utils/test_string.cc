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
 * Created Date: 2026-10-04
 * Author: AI
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "blockcache/utils/string.h"

namespace dingofs {
namespace blockcache {

TEST(StringTest, ConsumeUint) {
  {  // a block filename, <id>_<index>_<size>, field by field
    std::string_view name = "1001_3_4194304";
    uint64_t id = 0;
    uint32_t index = 0;
    uint32_t size = 0;
    ASSERT_TRUE(ConsumeUint(&name, '_', &id));
    EXPECT_EQ(id, 1001);
    EXPECT_EQ(name, "3_4194304");
    ASSERT_TRUE(ConsumeUint(&name, '_', &index));
    EXPECT_EQ(index, 3);
    EXPECT_EQ(name, "4194304");
    ASSERT_TRUE(ConsumeUint(&name, '\0', &size));  // '\0' takes the rest
    EXPECT_EQ(size, 4194304);
    EXPECT_TRUE(name.empty());
  }

  {  // a field that is missing or not a number fails and consumes nothing
    struct Case {
      std::string_view input;
      char sep;
    };
    const Case cases[] = {
        {.input = "", .sep = '\0'},
        {.input = "", .sep = '_'},
        {.input = "_3_4194304", .sep = '_'},
        {.input = "1001", .sep = '_'},
        {.input = "10a1_3_4194304", .sep = '_'},
        {.input = "-1001_3_4194304", .sep = '_'},
        {.input = " 1001_3_4194304", .sep = '_'},
        {.input = "4194304.tmp", .sep = '\0'},
        {.input = "18446744073709551616", .sep = '\0'},
    };
    for (const Case& c : cases) {
      std::string_view sv = c.input;
      uint64_t out = 0;
      EXPECT_FALSE(ConsumeUint(&sv, c.sep, &out)) << c.input;
      EXPECT_EQ(sv, c.input);
    }
  }

  {  // the field must fit the type it is parsed into
    std::string_view sv = "4294967295_0";
    uint32_t index = 0;
    ASSERT_TRUE(ConsumeUint(&sv, '_', &index));
    EXPECT_EQ(index, UINT32_MAX);

    sv = "4294967296_0";
    EXPECT_FALSE(ConsumeUint(&sv, '_', &index));
    EXPECT_EQ(sv, "4294967296_0");
  }

  {  // a missing uint32_t field fails the same way as a uint64_t one
    for (const std::string_view input : {"_0", "3"}) {
      std::string_view sv = input;
      uint32_t index = 0;
      EXPECT_FALSE(ConsumeUint(&sv, '_', &index)) << input;
      EXPECT_EQ(sv, input);
    }
  }
}

TEST(StringTest, SplitStr) {
  EXPECT_EQ(
      SplitStr("/data/cache1:102400,/data/cache2:204800", ','),
      (std::vector<std::string>{"/data/cache1:102400", "/data/cache2:204800"}));
  EXPECT_EQ(SplitStr("/data/cache1", ','),
            (std::vector<std::string>{"/data/cache1"}));

  {  // empty items are dropped, wherever they are
    EXPECT_EQ(SplitStr(",/data/cache1,,/data/cache2,", ','),
              (std::vector<std::string>{"/data/cache1", "/data/cache2"}));
    EXPECT_TRUE(SplitStr("", ',').empty());
    EXPECT_TRUE(SplitStr(",,,", ',').empty());
  }
}

TEST(StringTest, TrimWhitespace) {
  EXPECT_EQ(TrimWhitespace("10.0.0.2"), "10.0.0.2");
  EXPECT_EQ(TrimWhitespace(" \t10.0.0.2\r\n"), "10.0.0.2");
  EXPECT_EQ(TrimWhitespace("  dingo cache group  "), "dingo cache group");

  {  // nothing but whitespace
    EXPECT_EQ(TrimWhitespace(""), "");
    EXPECT_EQ(TrimWhitespace(" \t\r\n"), "");
  }
}

TEST(StringTest, RedString) {
  EXPECT_EQ(RedString("[required]"), "\x1B[31m[required]\033[0m");
  EXPECT_EQ(RedString(""), "\x1B[31m\033[0m");
}

}  // namespace blockcache
}  // namespace dingofs
