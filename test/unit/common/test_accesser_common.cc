/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#include "common/blockaccess/accesser_common.h"

#include <gflags/gflags.h>
#include <gtest/gtest.h>

namespace dingofs {
namespace blockaccess {

TEST(AccesserTypeTest, StringifiesKnownTypes) {
  EXPECT_EQ(AccesserType2Str(AccesserType::kS3), "S3");
  EXPECT_EQ(AccesserType2Str(AccesserType::kRados), "Rados");
  EXPECT_EQ(AccesserType2Str(AccesserType::kLocalFile), "LocalFile");
}

TEST(AccesserTypeTest, StringifiesUnknownTypeAsUnknown) {
  EXPECT_EQ(AccesserType2Str(static_cast<AccesserType>(255)), "Unknown");
}

TEST(ThrottleOptionsTest, FillsFromGFlagsCurrentValues) {
  gflags::FlagSaver saver;  // restore original flag values after this test
  FLAGS_iops_total_limit = 111;
  FLAGS_iops_read_limit = 22;
  FLAGS_iops_write_limit = 33;
  FLAGS_io_bandwidth_total_mb = 44;
  FLAGS_io_bandwidth_read_mb = 55;
  FLAGS_io_bandwidth_write_mb = 66;
  FLAGS_io_max_inflight_async_bytes = 7777;

  BlockAccesserThrottleOptions options;
  FillThrottleOptionsFromGFlags(&options);

  EXPECT_EQ(options.iopsTotalLimit, 111u);
  EXPECT_EQ(options.iopsReadLimit, 22u);
  EXPECT_EQ(options.iopsWriteLimit, 33u);
  EXPECT_EQ(options.bpsTotalMB, 44u);
  EXPECT_EQ(options.bpsReadMB, 55u);
  EXPECT_EQ(options.bpsWriteMB, 66u);
  EXPECT_EQ(options.maxAsyncRequestInflightBytes, 7777u);
}

}  // namespace blockaccess
}  // namespace dingofs
