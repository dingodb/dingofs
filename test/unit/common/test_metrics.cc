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

#include <butil/time.h>
#include <gtest/gtest.h>

#include "common/metrics/client/client.h"
#include "common/metrics/client/vfs/prefetch_metric.h"
#include "common/metrics/client/vfs/slice_metric.h"
#include "common/metrics/client/vfs/warmup_metric.h"
#include "common/metrics/mds/quota_metrics.h"
#include "common/metrics/metric.h"
#include "common/metrics/metric_guard.h"
#include "common/metrics/metrics_dumper.h"
#include "common/status.h"

namespace dingofs {
namespace metrics {

// bvar names below are exposed once per process (repeated construction just
// logs a harmless "already exposed" warning), so each aggregate is built
// exactly once and reused across the assertions in a single test.

TEST(OpMetricTest, ClientOpMetricGuardTracksSuccessAndFailure) {
  OpMetric metric("dingofs_test", "op_metric_guard_test");
  {
    ClientOpMetricGuard guard({&metric});
    EXPECT_EQ(metric.inflightOpNum.get_value(), 1);
  }
  EXPECT_EQ(metric.inflightOpNum.get_value(), 0);
  EXPECT_EQ(metric.qpsTotal.get_value(), 1u);
  EXPECT_EQ(metric.ecount.get_value(), 0u);

  {
    ClientOpMetricGuard guard({&metric});
    guard.FailOp();
  }
  EXPECT_EQ(metric.ecount.get_value(), 1u);
}

TEST(MetricGuardTest, StatusBasedGuardRecordsBpsAndQpsOnSuccess) {
  InterfaceMetric metric("dingofs_test", "status_guard_test");
  Status ok = Status::OK();
  {
    MetricGuard<Status> guard(&ok, &metric, /*count=*/128,
                              butil::cpuwide_time_us());
  }
  EXPECT_EQ(metric.qps.count.get_value(), 1u);
  EXPECT_EQ(metric.bps.count.get_value(), 128u);
  EXPECT_EQ(metric.eps.count.get_value(), 0u);
}

TEST(MetricGuardTest, StatusBasedGuardRecordsErrorOnFailure) {
  InterfaceMetric metric("dingofs_test", "status_guard_error_test");
  Status err = Status::Internal("boom");
  {
    MetricGuard<Status> guard(&err, &metric, /*count=*/1,
                              butil::cpuwide_time_us());
  }
  EXPECT_EQ(metric.eps.count.get_value(), 1u);
  EXPECT_EQ(metric.qps.count.get_value(), 0u);
}

TEST(MetricGuardTest, IntBasedGuardTreatsZeroAsSuccess) {
  InterfaceMetric metric("dingofs_test", "int_guard_test");
  int rc = 0;
  { MetricGuard<int> guard(&rc, &metric, 1, butil::cpuwide_time_us()); }
  EXPECT_EQ(metric.qps.count.get_value(), 1u);

  int failed_rc = -1;
  {
    MetricGuard<int> guard(&failed_rc, &metric, 1, butil::cpuwide_time_us());
  }
  EXPECT_EQ(metric.eps.count.get_value(), 1u);
}

TEST(MetricGuardTest, ListGuardUpdatesEveryMetricInList) {
  InterfaceMetric a("dingofs_test", "list_guard_a");
  InterfaceMetric b("dingofs_test", "list_guard_b");
  bool ok = true;
  {
    MetricListGuard guard(&ok, {&a, &b}, butil::cpuwide_time_us());
  }
  EXPECT_EQ(a.qps.count.get_value(), 1u);
  EXPECT_EQ(b.qps.count.get_value(), 1u);
}

TEST(MetricGuardTest, VFSRWMetricGuardSkipsUpdatesWhenDisabled) {
  InterfaceMetric metric("dingofs_test", "vfs_rw_guard_disabled_test");
  Status ok = Status::OK();
  size_t count = 42;
  { VFSRWMetricGuard guard(&ok, &metric, &count, /*enable=*/false); }
  EXPECT_EQ(metric.qps.count.get_value(), 0u);
}

TEST(MetricGuardTest, VFSRWMetricGuardRecordsWhenEnabled) {
  InterfaceMetric metric("dingofs_test", "vfs_rw_guard_enabled_test");
  Status ok = Status::OK();
  size_t count = 42;
  { VFSRWMetricGuard guard(&ok, &metric, &count, /*enable=*/true); }
  EXPECT_EQ(metric.qps.count.get_value(), 1u);
  EXPECT_EQ(metric.bps.count.get_value(), 42u);
}

TEST(MetricGuardTest, LatencyGuardRecordsLatency) {
  bvar::LatencyRecorder rec("dingofs_test_latency_guard");
  { LatencyGuard guard(&rec); }
  EXPECT_GE(rec.count(), 1);
}

TEST(SliceMetricTest, GuardTracksInflightAndCompletion) {
  client::SliceMetric metric;
  Status ok = Status::OK();
  {
    client::SliceMetricGuard guard(&ok, &metric.read_slice,
                                   butil::cpuwide_time_us());
    EXPECT_EQ(metric.read_slice.inflightOpNum.get_value(), 1);
  }
  EXPECT_EQ(metric.read_slice.inflightOpNum.get_value(), 0);
  EXPECT_EQ(metric.read_slice.qpsTotal.get_value(), 1u);
}

TEST(WarmupMetricTest, ConstructsWithZeroedCounters) {
  client::WarmupMetric metric;
  EXPECT_EQ(metric.inflight_warmup_tasks.get_value(), 0u);
  metric.inflight_warmup_files << 3;
  EXPECT_EQ(metric.inflight_warmup_files.get_value(), 3u);
}

TEST(PrefetchMetricTest, ConstructsWithZeroedCounter) {
  client::PrefetchMetric metric;
  EXPECT_EQ(metric.inflight_prefetch_blocks.get_value(), 0u);
}

TEST(ClientOpMetricTest, AllOperationCountersDefaultConstruct) {
  client::ClientOpMetric metric;
  EXPECT_EQ(metric.opRead.qpsTotal.get_value(), 0u);
  EXPECT_EQ(metric.opWrite.qpsTotal.get_value(), 0u);
}

TEST(FsQuotaMetricTest, UsageRatioIsZeroWithoutCapacity) {
  struct Source {
    static int64_t Zero(void*) { return 0; }
  };
  mds::FsQuotaMetric metric("dingofs_test_quota", static_cast<void*>(nullptr),
                            Source::Zero, Source::Zero, Source::Zero,
                            Source::Zero);
  EXPECT_DOUBLE_EQ(metric.get_inode_usage_ratio(), 0.0);
  EXPECT_DOUBLE_EQ(metric.get_bytes_usage_ratio(), 0.0);
}

TEST(FsQuotaMetricTest, UsageRatioDividesUsedByMax) {
  struct Source {
    static int64_t Used(void*) { return 50; }
    static int64_t Max(void*) { return 200; }
  };
  mds::FsQuotaMetric metric("dingofs_test_quota_ratio",
                            static_cast<void*>(nullptr), Source::Used,
                            Source::Used, Source::Max, Source::Max);
  EXPECT_DOUBLE_EQ(metric.get_inode_usage_ratio(), 0.25);
  EXPECT_DOUBLE_EQ(metric.get_bytes_usage_ratio(), 0.25);
}

TEST(MetricsDumperTest, DumpCollectsNameDescriptionPairs) {
  dingofs::client::MetricsDumper dumper;
  EXPECT_TRUE(dumper.dump("metric_a", butil::StringPiece("value_a")));
  EXPECT_TRUE(dumper.dump("metric_b", butil::StringPiece("value_b")));
  std::string contents = dumper.Contents();
  EXPECT_NE(contents.find("metric_a : value_a"), std::string::npos);
  EXPECT_NE(contents.find("metric_b : value_b"), std::string::npos);
}

}  // namespace metrics
}  // namespace dingofs
