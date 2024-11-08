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

#include "curvefs/src/client/filesystem/dir_quota_manager.h"

#include "curvefs/test/client/mock_dir_parent_watcher.h"
#include "curvefs/test/client/mock_inode_cache_manager.h"
#include "curvefs/test/client/mock_metaserver_client.h"
#include "curvefs/test/client/mock_timer.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace curvefs {
namespace client {
namespace filesystem {

using testing::Return;

using base::timer::MockTimer;
using rpcclient::MockMetaServerClient;

class DirQuotaManagerTest : public ::testing::Test {
 protected:
  std::shared_ptr<MockTimer> mock_timer;
  std::shared_ptr<MockMetaServerClient> mock_meta_client;

  std::shared_ptr<MockInodeCacheManager> inode_cache_manager;
  std::shared_ptr<DirParentWatcher> dir_parent_watcher;

  std::shared_ptr<DirQuotaManager> dir_quota_manager;

  void SetUp() override {
    mock_timer = std::make_shared<MockTimer>();
    mock_meta_client = std::make_shared<MockMetaServerClient>();

    inode_cache_manager = std::make_shared<MockInodeCacheManager>();
    dir_parent_watcher =
        std::make_shared<DirParentWatcherImpl>(inode_cache_manager);

    dir_quota_manager = std::make_shared<DirQuotaManager>(
        100, mock_meta_client, dir_parent_watcher, mock_timer);
  }
};

TEST_F(DirQuotaManagerTest, StartStop) {
  EXPECT_CALL(*mock_meta_client, LoadDirQuotas)
      .WillOnce(Return(MetaStatusCode::OK));

  EXPECT_CALL(*mock_timer, Add).WillRepeatedly(Return(true));

  dir_quota_manager->Start();
  EXPECT_TRUE(dir_quota_manager->IsRunning());

  dir_quota_manager->Stop();
}

TEST_F(DirQuotaManagerTest, HasDirQuota) {
  // dir 1
  //  - dir 10
  //   - dir 100
  //    - dir 1000
  //  - dir 20
  dir_parent_watcher->Remeber(1000, 100);
  dir_parent_watcher->Remeber(100, 10);
  dir_parent_watcher->Remeber(10, 1);
  dir_parent_watcher->Remeber(20, 1);

  EXPECT_CALL(*mock_meta_client, LoadDirQuotas)
      .WillOnce(
          [&](uint32_t fs_id, std::unordered_map<uint64_t, Quota>& dir_quotas) {
            EXPECT_EQ(fs_id, 100);
            {
              Quota quota;
              quota.set_maxbytes(500);
              quota.set_maxinodes(50);

              dir_quotas.emplace(10, quota);
            }
            {
              Quota quota;
              quota.set_maxbytes(1000);
              quota.set_maxinodes(100);

              dir_quotas.emplace(1000, quota);
            }

            return MetaStatusCode::OK;
          });

  EXPECT_CALL(*mock_timer, Add).WillRepeatedly(Return(true));

  dir_quota_manager->Start();
  EXPECT_TRUE(dir_quota_manager->IsRunning());

  {
    Ino quota_ino = 0;
    bool has = dir_quota_manager->NearestDirQuota(1000, quota_ino);
    EXPECT_TRUE(has);
    EXPECT_EQ(quota_ino, 1000);
  }

  {
    Ino quota_ino = 0;
    bool has = dir_quota_manager->NearestDirQuota(100, quota_ino);
    EXPECT_TRUE(has);
    EXPECT_EQ(quota_ino, 10);
  }

  {
    Ino quota_ino = 0;
    bool has = dir_quota_manager->NearestDirQuota(10, quota_ino);
    EXPECT_TRUE(has);
    EXPECT_EQ(quota_ino, 10);
  }

  {
    Ino quota_ino = 0;
    bool has = dir_quota_manager->NearestDirQuota(20, quota_ino);
    EXPECT_FALSE(has);
  }
}

TEST_F(DirQuotaManagerTest, CheckDirQuota) {
  // dir 1
  //  - dir 10
  //   - dir 100
  //    - dir 1000
  dir_parent_watcher->Remeber(1000, 100);
  dir_parent_watcher->Remeber(100, 10);
  dir_parent_watcher->Remeber(10, 1);

  EXPECT_CALL(*mock_meta_client, LoadDirQuotas)
      .WillOnce(
          [&](uint32_t fs_id, std::unordered_map<uint64_t, Quota>& dir_quotas) {
            EXPECT_EQ(fs_id, 100);
            {
              Quota quota;
              quota.set_maxbytes(500);
              quota.set_maxinodes(50);

              dir_quotas.emplace(10, quota);
            }
            {
              Quota quota;
              quota.set_maxbytes(1000);
              quota.set_maxinodes(100);

              dir_quotas.emplace(1000, quota);
            }

            return MetaStatusCode::OK;
          });

  EXPECT_CALL(*mock_timer, Add).WillRepeatedly(Return(true));

  dir_quota_manager->Start();
  EXPECT_TRUE(dir_quota_manager->IsRunning());

  {
    // check dir 1000
    {
      EXPECT_TRUE(dir_quota_manager->CheckDirQuota(1000, 400, 40));
      EXPECT_TRUE(dir_quota_manager->CheckDirQuota(1000, 500, 50));
    }

    {
      // parent quota not enough
      EXPECT_FALSE(dir_quota_manager->CheckDirQuota(1000, 500, 60));
      EXPECT_FALSE(dir_quota_manager->CheckDirQuota(1000, 600, 50));
    }
  }
  {
    // check dir 100
    EXPECT_TRUE(dir_quota_manager->CheckDirQuota(100, 500, 50));
    EXPECT_FALSE(dir_quota_manager->CheckDirQuota(100, 500, 51));
    EXPECT_FALSE(dir_quota_manager->CheckDirQuota(100, 601, 50));
  }
}

TEST_F(DirQuotaManagerTest, UpdateDirQuotaUsage) {
  // dir 1
  //  - dir 10
  //   - dir 100
  //    - dir 1000
  dir_parent_watcher->Remeber(1000, 100);
  dir_parent_watcher->Remeber(100, 10);
  dir_parent_watcher->Remeber(10, 1);

  EXPECT_CALL(*mock_meta_client, LoadDirQuotas)
      .WillOnce(
          [&](uint32_t fs_id, std::unordered_map<uint64_t, Quota>& dir_quotas) {
            EXPECT_EQ(fs_id, 100);
            {
              Quota quota;
              quota.set_maxbytes(500);
              quota.set_maxinodes(50);

              dir_quotas.emplace(10, quota);
            }
            {
              Quota quota;
              quota.set_maxbytes(1000);
              quota.set_maxinodes(100);

              dir_quotas.emplace(1000, quota);
            }

            return MetaStatusCode::OK;
          });

  EXPECT_CALL(*mock_timer, Add).WillRepeatedly(Return(true));

  dir_quota_manager->Start();
  EXPECT_TRUE(dir_quota_manager->IsRunning());

  EXPECT_TRUE(dir_quota_manager->CheckDirQuota(1000, 500, 50));

  EXPECT_TRUE(dir_quota_manager->CheckDirQuota(1000, 100, 10));
  dir_quota_manager->UpdateDirQuotaUsage(1000, 100, 10);

  EXPECT_FALSE(dir_quota_manager->CheckDirQuota(1000, 500, 50));
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs