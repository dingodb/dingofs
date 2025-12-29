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

#include "mds/statistics/fs_stat.h"

#include <cstdint>
#include <string>
#include <utility>

#include "common/logging.h"
#include "gflags/gflags.h"
#include "mds/common/codec.h"
#include "mds/common/status.h"
#include "mds/filesystem/store_operation.h"
#include "mds/storage/storage.h"

namespace dingofs {
namespace mds {

DEFINE_uint32(mds_fsstats_compact_interval_s, 3600, "compact fs stats interval seconds.");
DEFINE_uint32(mds_fsstats_duration_s, 60, "get per seconds fs stats duration.");

const uint64_t kNsPerSecond = 1000 * 1000 * 1000;

Status FsStats::UploadFsStat(Context& ctx, uint32_t fs_id, const FsStatsDataEntry& stats) {
  auto& trace = ctx.GetTrace();

  SaveFsStatsOperation operation(trace, fs_id, stats);

  return operation_processor_->RunAlone(&operation);
}

static void SumFsStats(const FsStatsDataEntry& src_stats, FsStatsDataEntry& dst_stats) {
  dst_stats.set_read_bytes(dst_stats.read_bytes() + src_stats.read_bytes());
  dst_stats.set_read_qps(dst_stats.read_qps() + src_stats.read_qps());
  dst_stats.set_write_bytes(dst_stats.write_bytes() + src_stats.write_bytes());
  dst_stats.set_write_qps(dst_stats.write_qps() + src_stats.write_qps());
  dst_stats.set_s3_read_bytes(dst_stats.s3_read_bytes() + src_stats.s3_read_bytes());
  dst_stats.set_s3_read_qps(dst_stats.s3_read_qps() + src_stats.s3_read_qps());
  dst_stats.set_s3_write_bytes(dst_stats.s3_write_bytes() + src_stats.s3_write_bytes());
  dst_stats.set_s3_write_qps(dst_stats.s3_write_qps() + src_stats.s3_write_qps());
}

Status FsStats::GetFsStat(Context& ctx, uint32_t fs_id, FsStatsDataEntry& stats) {
  auto& trace = ctx.GetTrace();

  uint64_t mark_time_ns = utils::TimestampUs() - FLAGS_mds_fsstats_duration_s * kNsPerSecond;
  GetAndCompactFsStatsOperation operation(trace, fs_id, mark_time_ns);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[fsstat.{}] get fs stats fail, {}.", fs_id, status.error_str());
    return status;
  }

  auto& result = operation.GetResult();
  stats = std::move(result.fs_stats);

  return Status::OK();
}

Status FsStats::GetFsStatsPerSecond(Context& ctx, uint32_t fs_id,
                                    std::map<uint64_t, FsStatsDataEntry>& stats_per_second) {
  auto& trace = ctx.GetTrace();

  FsStatsDataEntry sum_stats;
  uint64_t mark_time_s = utils::Timestamp() - FLAGS_mds_fsstats_duration_s;
  ScanFsStatsOperation operation(trace, fs_id, mark_time_s * kNsPerSecond,
                                 [&](const std::string& key, const std::string& value) -> bool {
                                   uint32_t fs_id = 0;
                                   uint64_t time_ns = 0;
                                   MetaCodec::DecodeFsStatsKey(key, fs_id, time_ns);

                                   uint64_t time_s = time_ns / kNsPerSecond;
                                   if (time_s == mark_time_s) {
                                     SumFsStats(MetaCodec::DecodeFsStatsValue(value), sum_stats);

                                   } else if (time_s > mark_time_s) {
                                     stats_per_second.insert(std::make_pair(mark_time_s, sum_stats));
                                     mark_time_s = time_s;
                                     sum_stats = MetaCodec::DecodeFsStatsValue(value);
                                   }

                                   return true;
                                 });

  return operation_processor_->RunAlone(&operation);
}

}  // namespace mds
}  // namespace dingofs