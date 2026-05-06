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

#include <cstdint>
#include <ctime>
#include <string>

#include "common/const.h"
#include "gtest/gtest.h"
#include "mds/common/trash.h"

namespace dingofs {
namespace mds {
namespace unit_test {

// ---- Trash inode constants ----

TEST(TrashInodeTest, NoCollisionWithReservedInodes) {
  EXPECT_NE(kTrashInodeId, kRootIno);
  EXPECT_NE(kTrashInodeId, kRootParentIno);
  EXPECT_NE(kTrashInodeId, kStatsIno);
  EXPECT_EQ(kTrashInodeId, kTrashIno);
}

TEST(TrashInodeTest, IsOddDirectory) {
  EXPECT_TRUE(kTrashInodeId & 1)
      << "kTrashInodeId must be odd for directory convention";
  EXPECT_TRUE(kTrashSubInodeStart & 1) << "kTrashSubInodeStart must be odd";
}

TEST(TrashInodeTest, IsTrashInodeBoundary) {
  EXPECT_FALSE(IsTrashInode(0));
  EXPECT_FALSE(IsTrashInode(1));
  EXPECT_FALSE(IsTrashInode(20000000000ULL));
  EXPECT_FALSE(IsTrashInode(kTrashInodeId - 1));
  EXPECT_TRUE(IsTrashInode(kTrashInodeId));
  EXPECT_TRUE(IsTrashInode(kTrashInodeId + 1));
  EXPECT_TRUE(IsTrashInode(kTrashSubInodeStart));
  EXPECT_TRUE(IsTrashInode(kTrashSubInodeStart + 100));
}

TEST(TrashInodeTest, IsInternalNode) { EXPECT_TRUE(IsInternalNode(kTrashIno)); }

TEST(TrashInodeTest, IsInternalName) {
  EXPECT_TRUE(IsInternalName(kTrashDirName));
  EXPECT_TRUE(IsInternalName(".trash"));
  EXPECT_FALSE(IsInternalName("trash"));
  EXPECT_FALSE(IsInternalName(".Trash"));
}

// ---- Trash entry name (build / parse) ----

TEST(TrashEntryNameTest, BuildAndParseRoundTrip) {
  // Covers basic build format, parse, and names containing dashes.
  Ino orig_parent = 100;
  Ino orig_file = 12345;
  std::string orig_name = "my-file-v2.txt";

  std::string entry = BuildTrashEntryName(orig_parent, orig_file, orig_name);
  EXPECT_EQ(entry, "100-12345-my-file-v2.txt");

  Ino parent = 0, file = 0;
  std::string name;
  EXPECT_TRUE(ParseTrashEntryName(entry, parent, file, name));
  EXPECT_EQ(parent, orig_parent);
  EXPECT_EQ(file, orig_file);
  EXPECT_EQ(name, orig_name);
}

TEST(TrashEntryNameTest, EmptyOriginalName) {
  std::string entry = BuildTrashEntryName(1, 2, "");
  EXPECT_EQ(entry, "1-2-");

  Ino parent = 0, file = 0;
  std::string name;
  EXPECT_TRUE(ParseTrashEntryName(entry, parent, file, name));
  EXPECT_EQ(parent, 1);
  EXPECT_EQ(file, 2);
  EXPECT_EQ(name, "");
}

TEST(TrashEntryNameTest, LargeInoValues) {
  Ino parent = 20000000001ULL;
  Ino file = 20000000002ULL;
  std::string original = "file.txt";

  std::string entry = BuildTrashEntryName(parent, file, original);

  Ino p = 0, f = 0;
  std::string name;
  EXPECT_TRUE(ParseTrashEntryName(entry, p, f, name));
  EXPECT_EQ(p, parent);
  EXPECT_EQ(f, file);
  EXPECT_EQ(name, original);
}

TEST(TrashEntryNameTest, BuildTruncation) {
  std::string long_name(300, 'a');
  std::string entry = BuildTrashEntryName(1, 2, long_name);
  EXPECT_LE(entry.size(), 255u);
}

TEST(TrashEntryNameTest, ParseInvalid) {
  Ino parent = 0, file = 0;
  std::string name;

  EXPECT_FALSE(ParseTrashEntryName("", parent, file, name));
  EXPECT_FALSE(ParseTrashEntryName("nodash", parent, file, name));
  EXPECT_FALSE(ParseTrashEntryName("123", parent, file, name));
  EXPECT_FALSE(ParseTrashEntryName("abc-def-name", parent, file, name));
}

// ---- Trash hour-bucket name (format / parse) ----

TEST(TrashBucketNameTest, FormatAndParseRoundTrip) {
  // 2026-04-05 14:30:00 UTC
  uint64_t ts = 1775399400;
  std::string name = FormatTrashBucketName(ts);
  EXPECT_EQ(name, "2026-04-05-14");

  uint64_t parsed = ParseTrashBucketName(name);
  // Parsed is rounded to the hour start: 2026-04-05 14:00:00 UTC
  EXPECT_EQ(parsed, 1775397600);
  EXPECT_EQ(FormatTrashBucketName(parsed), name);

  // Year-boundary buckets round-trip too.
  uint64_t midnight = ParseTrashBucketName("2026-01-01-00");
  EXPECT_GT(midnight, 0u);
  EXPECT_EQ(FormatTrashBucketName(midnight), "2026-01-01-00");
  uint64_t last_hour = ParseTrashBucketName("2026-12-31-23");
  EXPECT_GT(last_hour, 0u);
  EXPECT_EQ(FormatTrashBucketName(last_hour), "2026-12-31-23");
}

TEST(TrashBucketNameTest, ParseInvalid) {
  EXPECT_EQ(ParseTrashBucketName(""), 0u);
  EXPECT_EQ(ParseTrashBucketName("invalid"), 0u);
}

TEST(TrashBucketNameTest, RoundTripCurrentTime) {
  uint64_t now = static_cast<uint64_t>(std::time(nullptr));
  std::string bucket = FormatTrashBucketName(now);
  EXPECT_FALSE(bucket.empty());

  uint64_t parsed = ParseTrashBucketName(bucket);
  EXPECT_GT(parsed, 0u);
  // parsed is hour-aligned, so it should be <= now and within one hour.
  EXPECT_LE(parsed, now);
  EXPECT_GT(parsed + 3600, now);
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
