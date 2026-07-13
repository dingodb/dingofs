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

#include "common/const.h"
#include "common/meta.h"
#include "common/types.h"

namespace dingofs {

TEST(ConstTest, IsInternalNodeRecognizesReservedInodes) {
  EXPECT_TRUE(IsInternalNode(kStatsIno));
  EXPECT_TRUE(IsInternalNode(kRootIno));
  EXPECT_TRUE(IsInternalNode(kTrashIno));
  EXPECT_FALSE(IsInternalNode(12345));
}

TEST(ConstTest, IsInternalNameRecognizesReservedNames) {
  EXPECT_TRUE(IsInternalName(kStatsName));
  EXPECT_TRUE(IsInternalName(kTrashDirName));
  EXPECT_FALSE(IsInternalName("regular_file.txt"));
}

TEST(ConstTest, GenFsMetaTableNameWithoutClusterIdUsesFsNameOnly) {
  EXPECT_EQ(GenFsMetaTableName("myfs"), "dingofs-fsmeta[myfs]");
}

TEST(ConstTest, GenFsMetaTableNameWithClusterIdIncludesBothIds) {
  EXPECT_EQ(GenFsMetaTableName(7, "myfs"), "dingofs-fsmeta[7][myfs]");
}

TEST(ConstTest, HasDirAttrMutationIsTrueWhenBucketCountConfigured) {
  EXPECT_TRUE(HasDirAttrMutation());
}

TEST(TypesTest, ParseMetaSystemTypeMapsKnownStrings) {
  EXPECT_EQ(ParseMetaSystemType("mds"), MetaSystemType::MDS);
  EXPECT_EQ(ParseMetaSystemType("local"), MetaSystemType::LOCAL);
  EXPECT_EQ(ParseMetaSystemType("memory"), MetaSystemType::MEMORY);
}

TEST(TypesTest, ParseMetaSystemTypeMapsUnknownStringToUnknown) {
  EXPECT_EQ(ParseMetaSystemType("bogus"), MetaSystemType::UNKNOWN);
}

TEST(TypesTest, MetaSystemTypeToStringRoundTripsKnownValues) {
  EXPECT_EQ(MetaSystemTypeToString(MetaSystemType::MDS), "mds");
  EXPECT_EQ(MetaSystemTypeToString(MetaSystemType::LOCAL), "local");
  EXPECT_EQ(MetaSystemTypeToString(MetaSystemType::MEMORY), "memory");
}

TEST(TypesTest, MetaSystemTypeToStringMapsUnknownToUnknownLiteral) {
  EXPECT_EQ(MetaSystemTypeToString(MetaSystemType::UNKNOWN), "unknown");
}

}  // namespace dingofs
