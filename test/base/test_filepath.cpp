/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-09-04
 * Author: Jingli Chen (Wine93)
 */

#include "base/filepath/filepath.h"
#include "gtest/gtest.h"

namespace dingofs {
namespace base {
namespace filepath {

class FilepathTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(FilepathTest, ParentDir) {
  struct TestCase {
    std::string path;
    std::string parent;
  };

  std::vector<TestCase> tests{
      {"/a/b/c", "/a/b"}, {"/abc", "/"}, {"/a/b/c/", "/a/b/c"},
      {"/", "/"},         {"abc", "/"},  {"", "/"},
  };
  for (const auto& t : tests) {
    auto parent = ParentDir(t.path);
    ASSERT_EQ(parent, t.parent);
  }
}

TEST_F(FilepathTest, Filename) {
  struct TestCase {
    std::string path;
    std::string filename;
  };

  std::vector<TestCase> tests{
      {"/abc/def", "def"}, {"/abc", "abc"}, {"/a/b/c", "c"},
      {"/a/b/c/", ""},     {"/", ""},
  };
  for (const auto& t : tests) {
    auto filename = Filename(t.path);
    ASSERT_EQ(filename, t.filename);
  }
}

TEST_F(FilepathTest, HasSuffix) {
  struct TestCase {
    std::string str;
    std::string suffix;
    bool yes;
  };

  std::vector<TestCase> tests{
      {"abcde", "cde", true},       {"file.ckpt", ".ckpt", true},
      {"file.pt", ".pt", true},     {"abcde", "", true},
      {"abcde", "1abcde", false},   {"file.ckpt", ".mp4", false},
      {"file.pt", "filept", false},
  };
  for (const auto& t : tests) {
    bool yes = HasSuffix(t.str, t.suffix);
    ASSERT_EQ(yes, t.yes);
  }
}

TEST_F(FilepathTest, Split) {
  struct TestCase {
    std::string path;
    std::vector<std::string> out;
  };

  std::vector<TestCase> tests{
      {"/a/b/c", {"a", "b", "c"}},
      {"/a/b/c/", {"a", "b", "c"}},
      {"/abc/def", {"abc", "def"}},
      {"abc", {"abc"}},
      {"/", {}},
      {"///", {}},
  };
  for (const auto& t : tests) {
    auto out = PathSplit(t.path);
    ASSERT_EQ(out, t.out);
  }
}

TEST_F(FilepathTest, Join) {
  struct TestCase {
    std::vector<std::string> subpaths;
    std::string out;
  };

  std::vector<TestCase> tests{
      {{"a", "b", "c"}, "a/b/c"},
      {{"", "a", "b", "c"}, "/a/b/c"},
      {{"abc"}, "abc"},
      {{}, ""},
      {{"/a/b/c", "d/e/f"}, "/a/b/c/d/e/f"},
  };

  for (const auto& t : tests) {
    auto out = PathJoin(t.subpaths);
    ASSERT_EQ(out, t.out);
  }
}

}  // namespace filepath
}  // namespace base
}  // namespace dingofs
