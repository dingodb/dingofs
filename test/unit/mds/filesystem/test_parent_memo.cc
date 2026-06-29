// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "gtest/gtest.h"
#include "json/value.h"
#include "mds/filesystem/parent_memo.h"

namespace dingofs {
namespace mds {
namespace unit_test {

TEST(ParentMemoTest, RememberUpdateForgetAndJsonSummary) {
  ParentMemo memo(1);

  Ino parent = 0;
  EXPECT_FALSE(memo.GetParent(100, parent));
  EXPECT_EQ(memo.Size(), 0u);

  memo.Remeber(100, 1);
  ASSERT_TRUE(memo.GetParent(100, parent));
  EXPECT_EQ(parent, 1u);
  EXPECT_EQ(memo.Size(), 1u);
  EXPECT_EQ(memo.Bytes(), sizeof(Ino) * 2);

  memo.Remeber(100, 2);
  ASSERT_TRUE(memo.GetParent(100, parent));
  EXPECT_EQ(parent, 2u);
  EXPECT_EQ(memo.Size(), 1u);

  memo.Remeber(101, 2);
  Json::Value summary;
  memo.Summary(summary);
  EXPECT_EQ(summary["name"].asString(), "parentmemo");
  EXPECT_EQ(summary["count"].asUInt64(), 2u);
  EXPECT_EQ(summary["bytes"].asUInt64(), sizeof(Ino) * 4);
  EXPECT_EQ(summary["total_count"].asInt64(), 2);

  Json::Value desc;
  memo.DescribeByJson(desc);
  EXPECT_EQ(desc["count"].asUInt64(), 2u);

  memo.Forget(100);
  EXPECT_FALSE(memo.GetParent(100, parent));
  EXPECT_EQ(memo.Size(), 1u);
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
