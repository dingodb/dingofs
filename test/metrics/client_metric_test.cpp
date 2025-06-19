/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "metrics/blockaccess/s3_accesser.h"
#include "metrics/client/client.h"
#include "metrics/client/vfs_legacy/kv_client.h"
#include "metrics/client/vfs_legacy/s3_cache_manager.h"
#include "metrics/client/vfs_legacy/s3_chunk_info.h"
#include "metrics/client/vfs_legacy/warmup.h"
#include "metrics/mds/mds_client.h"
#include "metrics/metaserver/metaserver_client.h"

namespace dingofs {
namespace metric {

using ::dingofs::metrics::blockaccess::S3Metric;
using ::dingofs::metrics::client::ClientOpMetric;
using ::dingofs::metrics::client::FSMetric;
using ::dingofs::metrics::client::vfs_legacy::KVClientMetric;
using ::dingofs::metrics::client::vfs_legacy::S3ChunkInfoMetric;
using ::dingofs::metrics::client::vfs_legacy::S3MultiManagerMetric;
using ::dingofs::metrics::client::vfs_legacy::WarmupManagerS3Metric;
using ::dingofs::metrics::mds::MDSClientMetric;
using ::dingofs::metrics::metaserver::MetaServerClientMetric;

class ClientMetricTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(ClientMetricTest, test_prefix) {
  {
    const char* prefix = "dingofs_mds_client";
    ASSERT_EQ(0, ::strcmp(MDSClientMetric::prefix.c_str(), prefix));
  }

  {
    const char* prefix = "dingofs_metaserver_client";
    ASSERT_EQ(0, ::strcmp(MetaServerClientMetric::prefix.c_str(), prefix));
  }

  {
    const char* prefix = "dingofs_fuse";
    ASSERT_EQ(0, ::strcmp(ClientOpMetric::prefix.c_str(), prefix));
  }

  {
    const char* prefix = "dingofs_client_manager";
    ASSERT_EQ(0, ::strcmp(S3MultiManagerMetric::prefix.c_str(), prefix));
  }

  {
    const char* prefix = "dingofs_filesystem";
    ASSERT_EQ(0, ::strcmp(FSMetric::prefix.c_str(), prefix));
  }

  {
    const char* prefix = "dingofs_s3";
    ASSERT_EQ(0, ::strcmp(S3Metric::prefix.c_str(), prefix));
  }

  {
    const char* prefix = "dingofs_kvclient";
    ASSERT_EQ(0, ::strcmp(KVClientMetric::prefix.c_str(), prefix));
  }

  {
    const char* prefix = "inode_s3_chunk_info";
    ASSERT_EQ(0, ::strcmp(S3ChunkInfoMetric::prefix.c_str(), prefix));
  }

  {
    const char* prefix = "dingofs_warmup";
    ASSERT_EQ(0, ::strcmp(WarmupManagerS3Metric::prefix.c_str(), prefix));
  }
}

}  // namespace metric
}  // namespace dingofs
