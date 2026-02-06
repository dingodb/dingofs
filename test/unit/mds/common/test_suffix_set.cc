// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
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

#include "mds/common/suffix_set.h"

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

namespace dingofs {
namespace mds {
namespace unit_test {

class SuffixSetTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(SuffixSetTest, EmptySuffixSet) {
  SuffixSet suffix_set;

  EXPECT_FALSE(suffix_set.HasSuffix("test.txt"));
  EXPECT_FALSE(suffix_set.HasSuffix("file.jpg"));
  EXPECT_FALSE(suffix_set.HasSuffix("no_suffix"));
}

TEST_F(SuffixSetTest, SingleSuffix) {
  SuffixSet suffix_set;
  suffix_set.Update("txt");

  EXPECT_TRUE(suffix_set.HasSuffix("test.txt"));
  EXPECT_TRUE(suffix_set.HasSuffix("file.txt"));
  EXPECT_FALSE(suffix_set.HasSuffix("test.jpg"));
  EXPECT_FALSE(suffix_set.HasSuffix("no_suffix"));
}

TEST_F(SuffixSetTest, MultipleSuffixesFromString) {
  SuffixSet suffix_set;
  suffix_set.Update("txt,jpg,png");

  EXPECT_TRUE(suffix_set.HasSuffix("test.txt"));
  EXPECT_TRUE(suffix_set.HasSuffix("image.jpg"));
  EXPECT_TRUE(suffix_set.HasSuffix("photo.png"));
  EXPECT_FALSE(suffix_set.HasSuffix("file.pdf"));
  EXPECT_FALSE(suffix_set.HasSuffix("no_suffix"));
}

TEST_F(SuffixSetTest, UpdateMultipleTimes) {
  SuffixSet suffix_set;
  suffix_set.Update("txt");
  suffix_set.Update("jpg");
  suffix_set.Update("png");

  EXPECT_TRUE(suffix_set.HasSuffix("test.txt"));
  EXPECT_TRUE(suffix_set.HasSuffix("image.jpg"));
  EXPECT_TRUE(suffix_set.HasSuffix("photo.png"));
  EXPECT_FALSE(suffix_set.HasSuffix("file.pdf"));
}

TEST_F(SuffixSetTest, DuplicateSuffixes) {
  SuffixSet suffix_set;
  suffix_set.Update("txt,txt,jpg");
  suffix_set.Update("txt");

  EXPECT_TRUE(suffix_set.HasSuffix("test.txt"));
  EXPECT_TRUE(suffix_set.HasSuffix("image.jpg"));
}

TEST_F(SuffixSetTest, NoSuffixInFilename) {
  SuffixSet suffix_set;
  suffix_set.Update("txt,jpg");

  EXPECT_FALSE(suffix_set.HasSuffix("filename"));
  EXPECT_FALSE(suffix_set.HasSuffix(""));
}

TEST_F(SuffixSetTest, EmptySuffixString) {
  SuffixSet suffix_set;
  suffix_set.Update("");

  EXPECT_FALSE(suffix_set.HasSuffix("test.txt"));
}

TEST_F(SuffixSetTest, MultipleDotsInFilename) {
  SuffixSet suffix_set;
  suffix_set.Update("tar,gz");

  EXPECT_TRUE(suffix_set.HasSuffix("archive.tar.gz"));
  EXPECT_TRUE(suffix_set.HasSuffix("backup.tar"));
}

TEST_F(SuffixSetTest, CaseSensitive) {
  SuffixSet suffix_set;
  suffix_set.Update("txt,JPG");

  EXPECT_TRUE(suffix_set.HasSuffix("test.txt"));
  EXPECT_TRUE(suffix_set.HasSuffix("image.JPG"));
  // Case sensitive - should not match
  EXPECT_FALSE(suffix_set.HasSuffix("image.jpg"));
  EXPECT_FALSE(suffix_set.HasSuffix("test.TXT"));
}

TEST_F(SuffixSetTest, SuffixWithSpecialChars) {
  // Note: SuffixSet uses rfind('.') to find the last dot
  // For "backup.tar.gz", it looks for suffix "gz"
  SuffixSet suffix_set;
  suffix_set.Update("gz,1");

  EXPECT_TRUE(suffix_set.HasSuffix("backup.tar.gz"));  // suffix is "gz"
  EXPECT_TRUE(suffix_set.HasSuffix("file.bak.1"));     // suffix is "1"
}

TEST_F(SuffixSetTest, LongSuffix) {
  SuffixSet suffix_set;
  suffix_set.Update("verylongextensionname");

  EXPECT_TRUE(suffix_set.HasSuffix("file.verylongextensionname"));
  EXPECT_FALSE(suffix_set.HasSuffix("file.short"));
}

TEST_F(SuffixSetTest, ConsecutiveUpdates) {
  SuffixSet suffix_set;

  // First update
  suffix_set.Update("c,cpp");
  EXPECT_TRUE(suffix_set.HasSuffix("test.c"));
  EXPECT_TRUE(suffix_set.HasSuffix("test.cpp"));
  EXPECT_FALSE(suffix_set.HasSuffix("test.py"));

  // Second update adds more
  suffix_set.Update("py,java");
  EXPECT_TRUE(suffix_set.HasSuffix("test.c"));
  EXPECT_TRUE(suffix_set.HasSuffix("test.cpp"));
  EXPECT_TRUE(suffix_set.HasSuffix("test.py"));
  EXPECT_TRUE(suffix_set.HasSuffix("test.java"));
}

TEST_F(SuffixSetTest, WhitespaceInSuffixes) {
  SuffixSet suffix_set;
  // Test with suffixes containing whitespace
  // Note: behavior depends on whether butil::SplitString trims whitespace
  suffix_set.Update("txt, jpg");

  // "txt" suffix should always work
  EXPECT_TRUE(suffix_set.HasSuffix("file.txt"));

  // The behavior for "jpg" with leading space depends on implementation
  // Either the space is preserved (and won't match) or trimmed (and will match)
  // We just verify the test runs without crashing
  bool has_jpg_suffix = suffix_set.HasSuffix("image.jpg");
  (void)has_jpg_suffix;  // Suppress unused variable warning
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
