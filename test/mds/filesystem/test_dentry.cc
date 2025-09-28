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

#include <string>

#include "dingofs/mds.pb.h"
#include "fmt/core.h"
#include "gtest/gtest.h"
#include "mds/filesystem/dentry.h"

namespace dingofs {
namespace mds {
namespace unit_test {

const int64_t kFsId = 1000;

class DentryTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(DentryTest, Put) {
  Dentry dentry(kFsId, "file1", 1, 1000, pb::mds::FileType::FILE, 1212,
                nullptr);

  ASSERT_EQ("file1", dentry.Name());
  ASSERT_EQ(1000, dentry.INo());
  ASSERT_EQ(pb::mds::FileType::FILE, dentry.Type());
  ASSERT_EQ(1212, dentry.Flag());
  ASSERT_EQ(nullptr, dentry.Inode());
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
