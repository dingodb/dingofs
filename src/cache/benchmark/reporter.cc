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

#include "cache/benchmark/option.h"
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
      "%s: threads=%d fsid=%llu ino=%llu blksize=%llu blocks=%llu\n", FLAGS_op,
      FLAGS_threads, FLAGS_fsid, FLAGS_ino, FLAGS_blksize, FLAGS_blocks);

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
  auto avglat = stat->AvgLat() * 1.0 / 1e6;
  auto maxlat = stat->MaxLat() * 1.0 / 1e6;
  auto minlat = stat->MinLat() * 1.0 / 1e6;
  auto percent = total->Count() * 1.0 / (FLAGS_threads * FLAGS_blocks) * 100;

  std::cout << absl::StrFormat(
      "%9s  %s: %6llu op/s  %5lld MB/s  lat(%.6lf %.6lf %.6lf)\n",
      absl::StrFormat("[%.2lf%%]", percent), FLAGS_op, iops, bandwidth, avglat,
      maxlat, minlat);

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
  auto avglat = total->AvgLat() * 1.0 / 1e6;
  auto maxlat = total->MaxLat() * 1.0 / 1e6;
  auto minlat = total->MinLat() * 1.0 / 1e6;

  std::cout << "\n";
  std::cout << "Summary (" << FLAGS_threads << " workers):\n";
  std::cout << absl::StrFormat(
      "  Avg(%s):  %llu op/s  %lld MB/s  lat(%.6lf %.6lf %.6lf)\n", FLAGS_op,
      iops, bandwidth, avglat, maxlat, minlat);
}

}  // namespace cache
}  // namespace dingofs
