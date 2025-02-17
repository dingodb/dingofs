// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "client/vfs_old/filesystem/dir_parent_watcher.h"

#include "client/vfs_old/mock_inode_cache_manager.h"
#include "gmock/gmock.h"

namespace dingofs {
namespace client {
namespace filesystem {

using ::testing::Return;

class DirParentWatcherTest : public ::testing::Test {
 protected:
  std::shared_ptr<MockInodeCacheManager> inode_cache_manager;

  std::shared_ptr<DirParentWatcherImpl> dir_parent_watcher;

  void SetUp() override {
    inode_cache_manager = std::make_shared<MockInodeCacheManager>();
    dir_parent_watcher =
        std::make_shared<DirParentWatcherImpl>(inode_cache_manager);
  }

  void TearDown() override {}
};

TEST_F(DirParentWatcherTest, RememberParent) {
  Ino ino = 1;
  Ino parent = 2;

  dir_parent_watcher->Remeber(ino, parent);

  Ino retrieved_parent;
  EXPECT_EQ(dir_parent_watcher->GetParent(ino, retrieved_parent),
            DINGOFS_ERROR::OK);
  EXPECT_EQ(retrieved_parent, parent);
}

TEST_F(DirParentWatcherTest, ForgetParent) {
  EXPECT_CALL(*inode_cache_manager, GetInodeAttr)
      .WillOnce(Return(DINGOFS_ERROR::INTERNAL));

  Ino ino = 1;
  Ino parent = 2;

  dir_parent_watcher->Remeber(ino, parent);
  dir_parent_watcher->Forget(ino);

  Ino retrieved_parent;
  EXPECT_NE(dir_parent_watcher->GetParent(ino, retrieved_parent),
            DINGOFS_ERROR::OK);
}

TEST_F(DirParentWatcherTest, GetParentFromInodeCacheManager) {
  Ino ino = 1;
  Ino parent = 2;

  InodeAttr attr;
  attr.add_parent(parent);

  EXPECT_CALL(*inode_cache_manager, GetInodeAttr)
      .WillOnce([&](uint64_t inode_id, InodeAttr* out) {
        EXPECT_EQ(inode_id, ino);
        *out = attr;
        return DINGOFS_ERROR::OK;
      });

  Ino retrieved_parent;
  EXPECT_EQ(dir_parent_watcher->GetParent(ino, retrieved_parent),
            DINGOFS_ERROR::OK);
  EXPECT_EQ(retrieved_parent, parent);
}

TEST_F(DirParentWatcherTest, GetParentInodeAttrFailed) {
  Ino ino = 1;

  EXPECT_CALL(*inode_cache_manager, GetInodeAttr)
      .WillOnce(Return(DINGOFS_ERROR::INTERNAL));

  Ino retrieved_parent;
  EXPECT_EQ(dir_parent_watcher->GetParent(ino, retrieved_parent),
            DINGOFS_ERROR::INTERNAL);
}

TEST_F(DirParentWatcherTest, GetParentNoParentsInAttr) {
  Ino ino = 1;
  InodeAttr attr;

  EXPECT_CALL(*inode_cache_manager, GetInodeAttr)
      .WillOnce([&](uint64_t inode_id, InodeAttr* out) {
        EXPECT_EQ(inode_id, ino);
        *out = attr;
        return DINGOFS_ERROR::OK;
      });

  Ino retrieved_parent;
  EXPECT_EQ(dir_parent_watcher->GetParent(ino, retrieved_parent),
            DINGOFS_ERROR::NOTEXIST);
}

}  // namespace filesystem
}  // namespace client

}  // namespace dingofs
