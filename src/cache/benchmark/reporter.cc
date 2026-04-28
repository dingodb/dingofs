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

/*
 * Project: DingoFS
 * Created Date: 2025-06-04
 * Author: Jingli Chen (Wine93)
 */

#include "cache/benchmark/reporter.h"

#include <absl/strings/str_format.h>
#include <fmt/format.h>

#include <fstream>
#include <iostream>
#include <sstream>

#include "cache/benchmark/option.h"
#include "common/options/cache.h"
#include "common/options/cache.h"
#include "utils/executor/bthread/bthread_executor.h"

namespace dingofs {
namespace cache {

Reporter::Reporter(CollectorSPtr collector)
    : collector_(collector), executor_(std::make_unique<BthreadExecutor>()) {}

Status Reporter::Start() {
  if (!executor_->Start()) {
    return Status::Internal("start reporter timer failed");
  }
  g_timer_.start();

  collector_->Submit([this](Stat* stat, Stat* total) { OnStart(stat, total); });
  executor_->Schedule([this]() { TickTok(); }, kReportIntervalSeconds * 1000);
  return Status::OK();
}

void Reporter::Shutdown() {
  g_timer_.stop();

  executor_->Stop();
  collector_->Submit([this](Stat* stat, Stat* total) { OnStop(stat, total); });
}

void Reporter::TickTok() {
  collector_->Submit([this](Stat* stat, Stat* total) { OnShow(stat, total); });
  executor_->Schedule([this]() { TickTok(); }, kReportIntervalSeconds * 1000);
}

// put: threads=3 fsid=1 ino=1 blksize=4096 blocks=1000000
// ...
// Starting 3 workers
// ...
void Reporter::OnStart(Stat* stat, Stat* total) {
  CHECK_EQ(stat->Count(), 0);
  CHECK_EQ(total->Count(), 0);

  std::cout << absl::StrFormat(
      "%s: threads=%d fsid=%llu ino=%llu start_block_id=%llu blksize=%llu "
      "blocks=%llu offset=%llu length=%llu remote_only=%s use_rdma=%s "
      "registered_buffers=%s\n",
      FLAGS_op, FLAGS_threads, FLAGS_fsid, FLAGS_ino, FLAGS_start_block_id,
      FLAGS_blksize, FLAGS_blocks, FLAGS_offset, FLAGS_length,
      FLAGS_bench_remote_only ? "true" : "false",
      FLAGS_use_rdma ? "true" : "false",
      FLAGS_bench_rdma_registered_buffers ? "true" : "false");

  std::cout << "...\n";
  std::cout << "Starting " << FLAGS_threads << " workers\n";
  std::cout << "...\n";
}

// [10.28%]    put:    584 op/s   2336 MB/s  lat(0.013706 0.042489 0.002988)
// [10.28%]    put:    563 op/s   2253 MB/s  lat(0.014187 0.044417 0.003051)
void Reporter::OnShow(Stat* stat, Stat* total) {
  auto interval_us = kReportIntervalSeconds * 1e6;
  auto iops = stat->IOPS(interval_us);
  auto bandwidth = stat->Bandwidth(interval_us);
  auto percent =
      FLAGS_time_based
          ? 0
          : total->Count() * 1.0 / (FLAGS_threads * FLAGS_blocks) * 100;

  std::cout << absl::StrFormat(
      "%9s  %s: %9.2f qps  %9.2f MiB/s  ok/fail=%llu/%llu  "
      "lat_us(min/mean/p50/p90/p99/max=%llu/%llu/%llu/%llu/%llu/%llu)\n",
      FLAGS_time_based ? "[time]" : absl::StrFormat("[%.2lf%%]", percent),
      FLAGS_op, iops, bandwidth, stat->SuccessCount(), stat->FailCount(),
      stat->MinLat(), stat->AvgLat(), stat->PercentileLat(50),
      stat->PercentileLat(90), stat->PercentileLat(99), stat->MaxLat());

  *stat = Stat();  // Reset the interval stat
}

// Summary (3 workers):
//   avg(put):  563 op/s  2253 MB/s  lat(0.014187 0.044417 0.003051)
void Reporter::OnStop(Stat* stat, Stat* total) {
  if (stat->Count() != 0) {
    OnShow(stat, total);
  }

  auto interval_us = g_timer_.u_elapsed();
  auto iops = total->IOPS(interval_us);
  auto bandwidth = total->Bandwidth(interval_us);

  std::cout << "\n";
  std::cout << "Summary (" << FLAGS_threads << " workers):\n";
  std::cout << absl::StrFormat(
      "  Avg(%s):  %.2f qps  %.2f MiB/s  attempts=%llu success=%llu "
      "fail=%llu bytes=%llu\n",
      FLAGS_op, iops, bandwidth, total->Count(), total->SuccessCount(),
      total->FailCount(), total->TotalBytes());
  std::cout << absl::StrFormat(
      "  Lat(us): min=%llu mean=%llu p50=%llu p90=%llu p99=%llu max=%llu\n",
      total->MinLat(), total->AvgLat(), total->PercentileLat(50),
      total->PercentileLat(90), total->PercentileLat(99), total->MaxLat());

  if (FLAGS_json_result || !FLAGS_result_path.empty()) {
    auto json = BuildJson(*total, interval_us);
    if (FLAGS_json_result) {
      std::cout << json << "\n";
    }
    if (!FLAGS_result_path.empty()) {
      std::ofstream out(FLAGS_result_path);
      if (!out) {
        LOG(ERROR) << "Fail to open result_path=" << FLAGS_result_path;
      } else {
        out << json << "\n";
      }
    }
  }
}

std::string Reporter::BuildJson(const Stat& total,
                                uint64_t elapsed_us) const {
  std::ostringstream oss;
  oss << "{";
  oss << "\"op\":\"" << FLAGS_op << "\",";
  oss << "\"fsid\":" << FLAGS_fsid << ",";
  oss << "\"ino\":" << FLAGS_ino << ",";
  oss << "\"start_block_id\":" << FLAGS_start_block_id << ",";
  oss << "\"blksize\":" << FLAGS_blksize << ",";
  oss << "\"blocks\":" << FLAGS_blocks << ",";
  oss << "\"threads\":" << FLAGS_threads << ",";
  oss << "\"offset\":" << FLAGS_offset << ",";
  oss << "\"length\":" << FLAGS_length << ",";
  oss << "\"time_based\":" << (FLAGS_time_based ? "true" : "false") << ",";
  oss << "\"runtime\":" << FLAGS_runtime << ",";
  oss << "\"remote_only\":"
      << (FLAGS_bench_remote_only ? "true" : "false") << ",";
  oss << "\"use_rdma\":" << (FLAGS_use_rdma ? "true" : "false") << ",";
  oss << "\"registered_buffers\":"
      << (FLAGS_bench_rdma_registered_buffers ? "true" : "false") << ",";
  oss << "\"elapsed_us\":" << elapsed_us << ",";
  oss << "\"attempts\":" << total.Count() << ",";
  oss << "\"success\":" << total.SuccessCount() << ",";
  oss << "\"fail\":" << total.FailCount() << ",";
  oss << "\"bytes\":" << total.TotalBytes() << ",";
  oss << "\"qps\":" << total.IOPS(elapsed_us) << ",";
  oss << "\"mib_per_sec\":" << total.Bandwidth(elapsed_us) << ",";
  oss << "\"lat_us\":{";
  oss << "\"min\":" << total.MinLat() << ",";
  oss << "\"mean\":" << total.AvgLat() << ",";
  oss << "\"p50\":" << total.PercentileLat(50) << ",";
  oss << "\"p90\":" << total.PercentileLat(90) << ",";
  oss << "\"p99\":" << total.PercentileLat(99) << ",";
  oss << "\"max\":" << total.MaxLat();
  oss << "}";
  oss << "}";
  return oss.str();
}

}  // namespace cache
}  // namespace dingofs
