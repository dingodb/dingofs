/*
 *  Copyright (c) 2023 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: dingo
 * Created Date: Fri Apr 21 2023
 * Author: Xinlong-Chen
 */

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "stub/metric/metric.h"

namespace dingofs {
namespace stub {

using ::dingofs::stub::metric::ClientOpMetric;
using ::dingofs::stub::metric::DiskCacheMetric;
using ::dingofs::stub::metric::FSMetric;
using ::dingofs::stub::metric::KVClientMetric;
using ::dingofs::stub::metric::MDSClientMetric;
using ::dingofs::stub::metric::MetaServerClientMetric;
using ::dingofs::stub::metric::S3ChunkInfoMetric;
using ::dingofs::stub::metric::S3Metric;
using ::dingofs::stub::metric::S3MultiManagerMetric;
using ::dingofs::stub::metric::WarmupManagerS3Metric;

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
    const char* prefix = "dingofs_client";
    ASSERT_EQ(0, ::strcmp(ClientOpMetric::prefix.c_str(), prefix));
  }

  {
    const char* prefix = "dingofs_client_manager";
    ASSERT_EQ(0, ::strcmp(S3MultiManagerMetric::prefix.c_str(), prefix));
  }

  {
    const char* prefix = "dingofs_client";
    ASSERT_EQ(0, ::strcmp(FSMetric::prefix.c_str(), prefix));
  }

  {
    const char* prefix = "dingofs_s3";
    ASSERT_EQ(0, ::strcmp(S3Metric::prefix.c_str(), prefix));
  }

  {
    const char* prefix = "dingofs_disk_cache";
    ASSERT_EQ(0, ::strcmp(DiskCacheMetric::prefix.c_str(), prefix));
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

}  // namespace stub
}  // namespace dingofs
