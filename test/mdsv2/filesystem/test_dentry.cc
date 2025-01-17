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

#include <fmt/format.h>

#include <string>

#include "dingofs/mdsv2.pb.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/filesystem/dentry.h"

namespace dingofs {
namespace mdsv2 {
namespace unit_test {

const int64_t kFsId = 1000;

class DentryCacheTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(DentryCacheTest, Put) {
  DentryCache dentry_cache;

  Dentry root_dentry(kFsId, "root", 0, 1, pb::mdsv2::FileType::DIRECTORY, 0);

  auto dentry_set = DentrySet::New(root_dentry);

  uint64_t parent_ino = 1;
  dentry_set->PutChild(Dentry(kFsId, "dir01", parent_ino, 100000, pb::mdsv2::FileType::DIRECTORY, 0));
  dentry_set->PutChild(Dentry(kFsId, "dir02", parent_ino, 100001, pb::mdsv2::FileType::DIRECTORY, 0));
  dentry_set->PutChild(Dentry(kFsId, "dir03", parent_ino, 100002, pb::mdsv2::FileType::DIRECTORY, 0));
  dentry_set->PutChild(Dentry(kFsId, "dir04", parent_ino, 100003, pb::mdsv2::FileType::DIRECTORY, 0));
  dentry_set->PutChild(Dentry(kFsId, "file01", parent_ino, 100004, pb::mdsv2::FileType::FILE, 0));
  dentry_set->PutChild(Dentry(kFsId, "file01", parent_ino, 100005, pb::mdsv2::FileType::FILE, 0));

  dentry_cache.Put(root_dentry.Ino(), dentry_set);

  ASSERT_TRUE(dentry_cache.Get(root_dentry.Ino()) != nullptr);
}

TEST_F(DentryCacheTest, Delete) {
  DentryCache dentry_cache;

  Dentry root_dentry(kFsId, "root", 0, 1, pb::mdsv2::FileType::DIRECTORY, 0);

  auto dentry_set = DentrySet::New(root_dentry);

  uint64_t parent_ino = 1;
  dentry_set->PutChild(Dentry(kFsId, "dir01", parent_ino, 100000, pb::mdsv2::FileType::DIRECTORY, 0));
  dentry_set->PutChild(Dentry(kFsId, "dir02", parent_ino, 100001, pb::mdsv2::FileType::DIRECTORY, 0));
  dentry_set->PutChild(Dentry(kFsId, "dir03", parent_ino, 100002, pb::mdsv2::FileType::DIRECTORY, 0));
  dentry_set->PutChild(Dentry(kFsId, "dir04", parent_ino, 100003, pb::mdsv2::FileType::DIRECTORY, 0));
  dentry_set->PutChild(Dentry(kFsId, "file01", parent_ino, 100004, pb::mdsv2::FileType::FILE, 0));
  dentry_set->PutChild(Dentry(kFsId, "file01", parent_ino, 100005, pb::mdsv2::FileType::FILE, 0));

  dentry_cache.Put(root_dentry.Ino(), dentry_set);

  ASSERT_TRUE(dentry_cache.Get(root_dentry.Ino()) != nullptr);

  dentry_cache.Delete(root_dentry.Ino());
  ASSERT_TRUE(dentry_cache.Get(root_dentry.Ino()) == nullptr);
}

}  // namespace unit_test
}  // namespace mdsv2
}  // namespace dingofs
