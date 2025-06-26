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

#include "client/vfs_legacy/filesystem/fs_stat_manager.h"

#include <memory>

#include "client/vfs_legacy/mock_executor.h"
#include "client/vfs_legacy/mock_metaserver_client.h"
#include "dingofs/metaserver.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace dingofs {
namespace client {
namespace filesystem {

// using testing::_;
using testing::Return;

using dingofs::pb::metaserver::MetaStatusCode;
using dingofs::pb::metaserver::Quota;
using dingofs::pb::metaserver::Usage;
using dingofs::stub::rpcclient::MockMetaServerClient;

class FsStatManagerTest : public ::testing::Test {
 protected:
  std::shared_ptr<MockExecutor> mock_executor;
  std::shared_ptr<MockMetaServerClient> mock_meta_client;
  std::shared_ptr<FsStatManager> fs_stat_manager;

  void SetUp() override {
    mock_executor = std::make_shared<MockExecutor>();
    mock_meta_client = std::make_shared<MockMetaServerClient>();

    fs_stat_manager =
        std::make_shared<FsStatManager>(100, mock_meta_client, mock_executor);
  }

  void TearDown() override {
    // Clean up any resources used by the tests
  }
};

TEST_F(FsStatManagerTest, Start) {
  EXPECT_CALL(*mock_meta_client, GetFsQuota)
      .WillOnce(Return(MetaStatusCode::OK));

  EXPECT_CALL(*mock_executor, Schedule).WillOnce(Return(true));

  fs_stat_manager->Start();

  EXPECT_TRUE(fs_stat_manager->IsRunning());
}

TEST_F(FsStatManagerTest, Stop) {
  Quota fs_quota;
  fs_quota.set_maxbytes(1000);
  fs_quota.set_maxinodes(100);
  fs_quota.set_usedbytes(500);
  fs_quota.set_usedinodes(50);

  EXPECT_CALL(*mock_meta_client, GetFsQuota)
      .WillOnce([&](uint32_t fs_id, Quota& quota) {
        EXPECT_EQ(fs_id, 100);
        quota = fs_quota;
        return MetaStatusCode::OK;
      });

  EXPECT_CALL(*mock_meta_client, FlushFsUsage)
      .WillOnce([&](uint32_t fs_id, const Usage& usage, Quota& new_quota) {
        EXPECT_EQ(fs_id, 100);
        EXPECT_EQ(usage.bytes(), 0);
        EXPECT_EQ(usage.inodes(), 0);
        new_quota = fs_quota;
        return MetaStatusCode::OK;
      });

  EXPECT_CALL(*mock_executor, Schedule).WillOnce(Return(true));

  fs_stat_manager->Start();

  EXPECT_TRUE(fs_stat_manager->IsRunning());

  fs_stat_manager->Stop();

  EXPECT_FALSE(fs_stat_manager->IsRunning());
}

TEST_F(FsStatManagerTest, CheckQuota) {
  Quota fs_quota;
  fs_quota.set_maxbytes(1000);
  fs_quota.set_maxinodes(100);
  fs_quota.set_usedbytes(500);
  fs_quota.set_usedinodes(50);

  EXPECT_CALL(*mock_meta_client, GetFsQuota)
      .WillOnce([&](uint32_t fs_id, Quota& quota) {
        EXPECT_EQ(fs_id, 100);
        quota = fs_quota;
        return MetaStatusCode::OK;
      });

  EXPECT_CALL(*mock_executor, Schedule).WillOnce(Return(true));

  fs_stat_manager->Start();

  EXPECT_TRUE(fs_stat_manager->IsRunning());

  {
    uint64_t new_space = 100;
    uint64_t new_inodes = 10;

    bool result = fs_stat_manager->CheckFsQuota(new_space, new_inodes);
    EXPECT_TRUE(result);
  }

  {
    uint64_t new_space = 1000;
    uint64_t new_inodes = 100;

    bool result = fs_stat_manager->CheckFsQuota(new_space, new_inodes);
    EXPECT_FALSE(result);
  }

  {
    uint64_t new_space = 100;
    uint64_t new_inodes = 100;

    bool result = fs_stat_manager->CheckFsQuota(new_space, new_inodes);
    EXPECT_FALSE(result);
  }
}

TEST_F(FsStatManagerTest, UpdateQuotaUsage) {
  Quota fs_quota;
  fs_quota.set_maxbytes(1000);
  fs_quota.set_maxinodes(100);
  fs_quota.set_usedbytes(500);
  fs_quota.set_usedinodes(50);

  EXPECT_CALL(*mock_meta_client, GetFsQuota)
      .WillOnce([&](uint32_t fs_id, Quota& quota) {
        EXPECT_EQ(fs_id, 100);
        quota = fs_quota;
        return MetaStatusCode::OK;
      });

  EXPECT_CALL(*mock_executor, Schedule).WillOnce(Return(true));

  fs_stat_manager->Start();

  EXPECT_TRUE(fs_stat_manager->IsRunning());

  {
    uint64_t new_space = 100;
    uint64_t new_inodes = 10;

    bool result = fs_stat_manager->CheckFsQuota(new_space, new_inodes);
    EXPECT_TRUE(result);
  }

  {
    uint64_t new_space = 400;
    uint64_t new_inodes = 10;

    bool result = fs_stat_manager->CheckFsQuota(new_space, new_inodes);
    EXPECT_TRUE(result);

    // update quota usage
    fs_stat_manager->UpdateFsQuotaUsage(new_space, new_inodes);
  }

  {
    uint64_t new_space = 200;
    uint64_t new_inodes = 10;

    bool result = fs_stat_manager->CheckFsQuota(new_space, new_inodes);
    EXPECT_FALSE(result);
  }
}

TEST_F(FsStatManagerTest, DoFlushFsUsage) {
  Quota fs_quota;
  fs_quota.set_maxbytes(1000);
  fs_quota.set_maxinodes(100);
  fs_quota.set_usedbytes(500);
  fs_quota.set_usedinodes(50);

  EXPECT_CALL(*mock_meta_client, GetFsQuota)
      .WillOnce([&](uint32_t fs_id, Quota& quota) {
        EXPECT_EQ(fs_id, 100);
        quota = fs_quota;
        return MetaStatusCode::OK;
      });

  EXPECT_CALL(*mock_executor, Schedule).WillOnce(Return(true));

  {
    fs_stat_manager->Start();

    EXPECT_TRUE(fs_stat_manager->IsRunning());
  }

  uint64_t new_space = 400;
  uint64_t new_inodes = 10;

  {
    bool result = fs_stat_manager->CheckFsQuota(new_space, new_inodes);
    EXPECT_TRUE(result);

    // update quota usage
    fs_stat_manager->UpdateFsQuotaUsage(new_space, new_inodes);
  }

  {
    EXPECT_CALL(*mock_meta_client, FlushFsUsage)
        .WillOnce([&](uint32_t fs_id, const Usage& usage, Quota& new_quota) {
          EXPECT_EQ(fs_id, 100);
          EXPECT_EQ(usage.bytes(), 400);
          EXPECT_EQ(usage.inodes(), 10);
          Quota fs_new_quota;
          fs_new_quota.set_maxbytes(1000);
          fs_new_quota.set_maxinodes(100);
          fs_new_quota.set_usedbytes(900);
          fs_new_quota.set_usedinodes(60);
          new_quota = fs_new_quota;
          return MetaStatusCode::OK;
        });

    fs_stat_manager->Stop();

    EXPECT_FALSE(fs_stat_manager->IsRunning());
  }
}

}  // namespace filesystem

}  // namespace client

}  // namespace dingofs