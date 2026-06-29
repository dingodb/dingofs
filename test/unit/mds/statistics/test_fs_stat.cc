// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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

#include <cstdint>
#include <map>

#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "mds/common/codec.h"
#include "mds/common/context.h"
#include "mds/filesystem/store_operation.h"
#include "mds/statistics/fs_stat.h"
#include "mds/storage/dummy_storage.h"
#include "utils/time.h"

namespace dingofs {
namespace mds {

DECLARE_uint32(mds_fsstats_duration_s);

namespace unit_test {
namespace {

constexpr uint32_t kFsId = 1001;
constexpr uint64_t kNsPerSecond = 1000ULL * 1000ULL * 1000ULL;

FsStatsDataEntry MakeStats(uint64_t base) {
  FsStatsDataEntry stats;
  stats.set_read_bytes(base);
  stats.set_read_qps(base + 1);
  stats.set_write_bytes(base + 2);
  stats.set_write_qps(base + 3);
  stats.set_s3_read_bytes(base + 4);
  stats.set_s3_read_qps(base + 5);
  stats.set_s3_write_bytes(base + 6);
  stats.set_s3_write_qps(base + 7);
  return stats;
}

void ExpectStatsEq(const FsStatsDataEntry& stats, uint64_t base) {
  EXPECT_EQ(stats.read_bytes(), base);
  EXPECT_EQ(stats.read_qps(), base + 1);
  EXPECT_EQ(stats.write_bytes(), base + 2);
  EXPECT_EQ(stats.write_qps(), base + 3);
  EXPECT_EQ(stats.s3_read_bytes(), base + 4);
  EXPECT_EQ(stats.s3_read_qps(), base + 5);
  EXPECT_EQ(stats.s3_write_bytes(), base + 6);
  EXPECT_EQ(stats.s3_write_qps(), base + 7);
}

void ExpectStatsSum(const FsStatsDataEntry& stats, const FsStatsDataEntry& a,
                    const FsStatsDataEntry& b) {
  EXPECT_EQ(stats.read_bytes(), a.read_bytes() + b.read_bytes());
  EXPECT_EQ(stats.read_qps(), a.read_qps() + b.read_qps());
  EXPECT_EQ(stats.write_bytes(), a.write_bytes() + b.write_bytes());
  EXPECT_EQ(stats.write_qps(), a.write_qps() + b.write_qps());
  EXPECT_EQ(stats.s3_read_bytes(), a.s3_read_bytes() + b.s3_read_bytes());
  EXPECT_EQ(stats.s3_read_qps(), a.s3_read_qps() + b.s3_read_qps());
  EXPECT_EQ(stats.s3_write_bytes(), a.s3_write_bytes() + b.s3_write_bytes());
  EXPECT_EQ(stats.s3_write_qps(), a.s3_write_qps() + b.s3_write_qps());
}

}  // namespace

class FsStatsTest : public testing::Test {
 protected:
  void SetUp() override {
    old_duration_s_ = FLAGS_mds_fsstats_duration_s;
    FLAGS_mds_fsstats_duration_s = 60;

    storage_ = DummyStorage::New();
    ASSERT_TRUE(storage_->Init(""));

    operation_processor_ = OperationProcessor::New(storage_);
    ASSERT_TRUE(operation_processor_->Init());
    fs_stats_ = FsStats::New(operation_processor_);
  }

  void TearDown() override {
    fs_stats_.reset();
    if (operation_processor_ != nullptr) {
      operation_processor_->Destroy();
    }
    operation_processor_.reset();
    storage_.reset();
    FLAGS_mds_fsstats_duration_s = old_duration_s_;
  }

  void PutStats(uint64_t time_ns, const FsStatsDataEntry& stats) {
    ASSERT_TRUE(storage_
                    ->Put(KVStorage::WriteOption(),
                          MetaCodec::EncodeFsStatsKey(kFsId, time_ns),
                          MetaCodec::EncodeFsStatsValue(stats))
                    .ok());
  }

  KVStorageSPtr storage_;
  OperationProcessorSPtr operation_processor_;
  FsStatsUPtr fs_stats_;
  uint32_t old_duration_s_{0};
};

TEST_F(FsStatsTest, UploadAndGetAggregatesStats) {
  Context ctx;
  auto first = MakeStats(10);
  auto second = MakeStats(100);

  ASSERT_TRUE(fs_stats_->UploadFsStat(ctx, kFsId, first).ok());
  ASSERT_TRUE(fs_stats_->UploadFsStat(ctx, kFsId, second).ok());

  FsStatsDataEntry stats;
  ASSERT_TRUE(fs_stats_->GetFsStat(ctx, kFsId, stats).ok());

  ExpectStatsSum(stats, first, second);
}

TEST_F(FsStatsTest, GetFsStatDeletesExpiredRawStatsButKeepsFreshStats) {
  FLAGS_mds_fsstats_duration_s = 10;
  const uint64_t now_s = utils::Timestamp();
  const uint64_t old_time_ns = (now_s - 20) * kNsPerSecond;
  const uint64_t fresh_time_ns = (now_s - 1) * kNsPerSecond;
  auto old_stats = MakeStats(1);
  auto fresh_stats = MakeStats(20);
  PutStats(old_time_ns, old_stats);
  PutStats(fresh_time_ns, fresh_stats);

  Context ctx;
  FsStatsDataEntry stats;
  ASSERT_TRUE(fs_stats_->GetFsStat(ctx, kFsId, stats).ok());
  ExpectStatsSum(stats, old_stats, fresh_stats);

  std::string value;
  auto old_status =
      storage_->Get(MetaCodec::EncodeFsStatsKey(kFsId, old_time_ns), value);
  EXPECT_FALSE(old_status.ok());
  EXPECT_EQ(old_status.error_code(), pb::error::ENOT_FOUND);
  ASSERT_TRUE(
      storage_->Get(MetaCodec::EncodeFsStatsKey(kFsId, fresh_time_ns), value)
          .ok());
}

TEST_F(FsStatsTest, GetFsStatsPerSecondReturnsEverySecondIncludingLastBucket) {
  const uint64_t now_s = utils::Timestamp();
  const uint64_t first_second = now_s - 2;
  const uint64_t second_second = now_s - 1;
  auto first = MakeStats(10);
  auto second = MakeStats(20);
  auto third = MakeStats(100);

  PutStats(first_second * kNsPerSecond + 100, first);
  PutStats(first_second * kNsPerSecond + 200, second);
  PutStats(second_second * kNsPerSecond + 100, third);

  Context ctx;
  std::map<uint64_t, FsStatsDataEntry> stats_per_second;
  ASSERT_TRUE(
      fs_stats_->GetFsStatsPerSecond(ctx, kFsId, stats_per_second).ok());

  ASSERT_EQ(stats_per_second.size(), 2u);
  ExpectStatsSum(stats_per_second.at(first_second), first, second);
  ExpectStatsEq(stats_per_second.at(second_second), 100);
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
