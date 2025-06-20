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

#include "client/vfs_legacy/filesystem/fs_push_metric_manager.h"

#include <iostream>
#include <memory>

#include "stub/rpcclient/mock_mds_client.h"
#include "client/vfs_legacy/mock_timer.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace dingofs {
namespace client {
namespace filesystem {

// using testing::_;
using testing::Return;

using base::timer::MockTimer;
using dingofs::pb::mds::FsStatsData;
using dingofs::stub::rpcclient::MockMdsClient;

class FsPushMetricManagerTest : public ::testing::Test {
 protected:
  std::shared_ptr<MockTimer> mock_timer;
  std::shared_ptr<MockMdsClient> mock_mds_client;
  std::unique_ptr<FsPushMetricManager> fs_push_metrics_manager;

  void SetUp() override {
    mock_timer = std::make_shared<MockTimer>();
    mock_mds_client = std::make_shared<MockMdsClient>();

    const std::string fsname = "dingofs";
    fs_push_metrics_manager = std::make_unique<FsPushMetricManager>(
        fsname, mock_mds_client, mock_timer);
  }

  void TearDown() override {
    // Clean up any resources used by the tests
  }
};

TEST_F(FsPushMetricManagerTest, Start) {
  EXPECT_CALL(*mock_timer, Add).WillOnce(Return(true));

  fs_push_metrics_manager->Start();
  EXPECT_TRUE(fs_push_metrics_manager->IsRunning());
}

TEST_F(FsPushMetricManagerTest, Stop) {
  EXPECT_CALL(*mock_timer, Add).WillOnce(Return(true));

  fs_push_metrics_manager->Start();
  EXPECT_TRUE(fs_push_metrics_manager->IsRunning());

  EXPECT_CALL(*mock_mds_client, SetFsStats)
      .WillOnce(
          [&](const std::string& fsname, const FsStatsData& new_fs_stats_data) {
            EXPECT_EQ(fsname, std::string("dingofs"));
            return pb::mds::FSStatusCode::OK;
          });
  fs_push_metrics_manager->Stop();
  EXPECT_FALSE(fs_push_metrics_manager->IsRunning());
}

TEST_F(FsPushMetricManagerTest, DoPushClientMetrics) {
  FsStatsData last_fs_stats_data;
  last_fs_stats_data.set_readbytes(8192);
  last_fs_stats_data.set_writebytes(16384);
  last_fs_stats_data.set_readqps(10);
  last_fs_stats_data.set_writeqps(20);
  last_fs_stats_data.set_s3readbytes(8192);
  last_fs_stats_data.set_s3writebytes(16384);
  last_fs_stats_data.set_s3readqps(30);
  last_fs_stats_data.set_s3writeqps(40);

  FsStatsData cur_fs_stats_data;
  cur_fs_stats_data.set_readbytes(16384);
  cur_fs_stats_data.set_writebytes(16384);
  cur_fs_stats_data.set_readqps(20);
  cur_fs_stats_data.set_writeqps(20);
  cur_fs_stats_data.set_s3readbytes(16384);
  cur_fs_stats_data.set_s3writebytes(16384);
  cur_fs_stats_data.set_s3readqps(50);
  cur_fs_stats_data.set_s3writeqps(40);

  FsStatsData delta_client_metrics = cur_fs_stats_data - last_fs_stats_data;
  EXPECT_EQ(delta_client_metrics.readbytes(), 8192);
  EXPECT_EQ(delta_client_metrics.readqps(), 10);
  EXPECT_EQ(delta_client_metrics.s3readbytes(), 8192);
  EXPECT_EQ(delta_client_metrics.s3readqps(), 20);

  EXPECT_CALL(*mock_mds_client, SetFsStats)
      .WillOnce(
          [&](const std::string& fsname, const FsStatsData& new_fs_stats_data) {
            EXPECT_EQ(fsname, std::string("dingofs"));
            EXPECT_EQ(new_fs_stats_data.readbytes(), 8192);
            EXPECT_EQ(new_fs_stats_data.readqps(), 10);
            EXPECT_EQ(new_fs_stats_data.s3readbytes(), 8192);
            EXPECT_EQ(new_fs_stats_data.s3readqps(), 20);
            return pb::mds::FSStatusCode::OK;
          });
  std::string fsname = "dingofs";
  pb::mds::FSStatusCode ret =
      mock_mds_client->SetFsStats(fsname, delta_client_metrics);
  EXPECT_EQ(ret, pb::mds::FSStatusCode::OK);

  fs_push_metrics_manager->Stop();
  EXPECT_FALSE(fs_push_metrics_manager->IsRunning());
}

}  // namespace filesystem

}  // namespace client

}  // namespace dingofs