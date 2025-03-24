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

#include "mdsv2/statistics/fs_stat.h"

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "gflags/gflags.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/common/status.h"
#include "mdsv2/filesystem/codec.h"
#include "mdsv2/storage/storage.h"

namespace dingofs {
namespace mdsv2 {

DECLARE_uint32(txn_max_retry_times);
DECLARE_int32(fs_scan_batch_size);

DEFINE_uint32(fs_stats_compact_interval_s, 3600, "compact fs stats interval seconds.");
DEFINE_uint32(fs_stats_duration_s, 60, "get per seconds fs stats duration.");

Status FsStats::UploadFsStat(Context& ctx, uint32_t fs_id, const pb::mdsv2::FsStatsData& stats) {
  auto& trace_txn = ctx.GetTrace().GetTxn();

  uint64_t now_ns = Helper::TimestampNs();
  std::string key = MetaDataCodec::EncodeFsStatsKey(fs_id, now_ns);

  Status status;
  int retry = 0;
  do {
    auto txn = kv_storage_->NewTxn();
    txn->Put(key, MetaDataCodec::EncodeFsStatsValue(stats));

    status = txn->Commit();
    trace_txn = txn->GetTrace();
    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }
    ++retry;

  } while (retry < FLAGS_txn_max_retry_times);

  trace_txn.retry = retry;

  return status;
}

static void SumFsStats(const pb::mdsv2::FsStatsData& src_stats, pb::mdsv2::FsStatsData& dst_stats) {
  dst_stats.set_read_bytes(dst_stats.read_bytes() + src_stats.read_bytes());
  dst_stats.set_read_qps(dst_stats.read_qps() + src_stats.read_qps());
  dst_stats.set_write_bytes(dst_stats.write_bytes() + src_stats.write_bytes());
  dst_stats.set_write_qps(dst_stats.write_qps() + src_stats.write_qps());
  dst_stats.set_s3_read_bytes(dst_stats.s3_read_bytes() + src_stats.s3_read_bytes());
  dst_stats.set_s3_read_qps(dst_stats.s3_read_qps() + src_stats.s3_read_qps());
  dst_stats.set_s3_write_bytes(dst_stats.s3_write_bytes() + src_stats.s3_write_bytes());
  dst_stats.set_s3_write_qps(dst_stats.s3_write_qps() + src_stats.s3_write_qps());
}

Status FsStats::GetFsStat(Context& ctx, uint32_t fs_id, pb::mdsv2::FsStatsData& stats) {
  auto& trace_txn = ctx.GetTrace().GetTxn();

  Range range;
  MetaDataCodec::GetFsStatsRange(fs_id, range.start_key, range.end_key);

  auto txn = kv_storage_->NewTxn();

  std::vector<KeyValue> kvs;
  auto status = txn->Scan(range, FLAGS_fs_scan_batch_size, kvs);
  if (!status.ok()) {
    return status;
  }

  uint64_t mart_time_ns = Helper::TimestampNs() - FLAGS_fs_stats_duration_s * 1000000000;
  std::string mark_key = MetaDataCodec::EncodeFsStatsKey(fs_id, mart_time_ns);
  pb::mdsv2::FsStatsData compact_stats;
  bool compacted = false;
  for (auto& kv : kvs) {
    // compact old stats
    if (kv.key <= mark_key) {
      txn->Delete(kv.key);

    } else if (!compacted) {
      compact_stats = stats;
      compacted = true;
    }

    // sum all stats
    SumFsStats(MetaDataCodec::DecodeFsStatsValue(kv.value), stats);
  }

  // put compact stats
  txn->Put(mark_key, MetaDataCodec::EncodeFsStatsValue(compact_stats));

  status = txn->Commit();
  trace_txn = txn->GetTrace();
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("[fsstat.{}] commit fs stats fail, {}.", fs_id, status.error_str());
  }

  return Status::OK();
}

Status FsStats::GetFsStatsPerSecond(Context& ctx, uint32_t fs_id,
                                    std::map<uint64_t, pb::mdsv2::FsStatsData>& stats_per_second) {
  auto& trace_txn = ctx.GetTrace().GetTxn();

  Range range;
  MetaDataCodec::GetFsStatsRange(fs_id, range.start_key, range.end_key);
  uint64_t start_time_s = Helper::Timestamp() - FLAGS_fs_stats_duration_s;
  range.start_key = MetaDataCodec::EncodeFsStatsKey(fs_id, start_time_s);

  auto txn = kv_storage_->NewTxn();

  std::vector<KeyValue> kvs;
  auto status = txn->Scan(range, FLAGS_fs_scan_batch_size, kvs);
  if (!status.ok()) {
    return status;
  }

  // calculate stats per second
  uint64_t mark_time_s = start_time_s;
  pb::mdsv2::FsStatsData sum_stats;
  for (auto& kv : kvs) {
    uint32_t fs_id = 0;
    uint64_t time = 0;
    MetaDataCodec::DecodeFsStatsKey(kv.key, fs_id, time);
    uint64_t time_s = time / 1000000000;
    if (time_s == mark_time_s) {
      SumFsStats(MetaDataCodec::DecodeFsStatsValue(kv.value), sum_stats);

    } else if (time_s > mark_time_s) {
      stats_per_second.insert(std::make_pair(mark_time_s, sum_stats));
      mark_time_s = time_s;
      sum_stats = MetaDataCodec::DecodeFsStatsValue(kv.value);
    }
  }

  status = txn->Commit();
  trace_txn = txn->GetTrace();
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("[fsstat.{}] commit fs stats fail, {}.", fs_id, status.error_str());
  }

  return Status::OK();
}

}  // namespace mdsv2
}  // namespace dingofs