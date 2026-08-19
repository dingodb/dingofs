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

#include "blockcache/tools/benchmark/reporter.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <algorithm>
#include <iostream>

#include "blockcache/tools/benchmark/option.h"
#include "utils/executor/bthread/bthread_executor.h"

namespace dingofs {
namespace blockcache {

Reporter::Reporter(CollectorSPtr collector)
    : collector_(collector), executor_(std::make_unique<BthreadExecutor>()) {}

Status Reporter::Start() {
  if (!executor_->Start()) {
    return Status::Internal("start reporter timer failed");
  }
  start_ = std::chrono::steady_clock::now();

  collector_->Submit([this](Stat* stat, Stat* total) { OnStart(stat, total); });
  executor_->Schedule([this]() { TickTok(); }, kReportIntervalSeconds * 1000);
  return Status::OK();
}

void Reporter::Shutdown() {
  executor_->Stop();
  collector_->Submit([this](Stat* stat, Stat* total) { OnStop(stat, total); });
}

void Reporter::TickTok() {
  collector_->Submit([this](Stat* stat, Stat* total) { OnShow(stat, total); });
  executor_->Schedule([this]() { TickTok(); }, kReportIntervalSeconds * 1000);
}

uint64_t Reporter::ElapsedUs() const {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::steady_clock::now() - start_)
      .count();
}

double Reporter::Percent(const Stat* total) const {
  double percent;
  if (FLAGS_time_based) {
    percent = ElapsedUs() * 100.0 / (FLAGS_runtime * 1e6);
  } else {
    percent = total->Count() * 100.0 / (FLAGS_threads * FLAGS_blocks);
  }
  return std::min(percent, 100.0);
}

void Reporter::OnStart(Stat* stat, Stat* total) {
  CHECK_EQ(stat->Count(), 0);
  CHECK_EQ(total->Count(), 0);

  std::cout << fmt::format(
      "{}: threads={} iodepth={} fsid={} blksize={} blocks={} time_based={} "
      "runtime={}\n",
      FLAGS_op, FLAGS_threads, FLAGS_iodepth, FLAGS_fsid, FLAGS_blksize,
      FLAGS_blocks, FLAGS_time_based, FLAGS_runtime);

  std::cout << "...\n";
  std::cout << "Starting " << FLAGS_threads << " workers\n";
  std::cout << "...\n";
}

void Reporter::OnShow(Stat* stat, Stat* total) {
  auto interval_us = kReportIntervalSeconds * 1e6;
  auto iops = stat->IOPS(interval_us);
  auto bandwidth = stat->Bandwidth(interval_us);
  auto avglat = stat->AvgLat() * 1.0 / 1e6;
  auto maxlat = stat->MaxLat() * 1.0 / 1e6;
  auto minlat = stat->MinLat() * 1.0 / 1e6;

  std::cout << fmt::format(
      "{:>9}  {}: {:>6} op/s  {:>5} MB/s  lat({:.6f} {:.6f} {:.6f})\n",
      fmt::format("[{:.2f}%]", Percent(total)), FLAGS_op, iops, bandwidth,
      avglat, maxlat, minlat);

  *stat = Stat();
}

void Reporter::OnStop(Stat* stat, Stat* total) {
  if (stat->Count() != 0) {
    OnShow(stat, total);
  }

  auto interval_us = ElapsedUs();
  auto iops = total->IOPS(interval_us);
  auto bandwidth = total->Bandwidth(interval_us);
  auto avglat = total->AvgLat() * 1.0 / 1e6;
  auto maxlat = total->MaxLat() * 1.0 / 1e6;
  auto minlat = total->MinLat() * 1.0 / 1e6;

  std::cout << "\n";
  std::cout << "Summary (" << FLAGS_threads << " workers):\n";
  std::cout << fmt::format(
      "  Avg({}):  {} op/s  {} MB/s  lat({:.6f} {:.6f} {:.6f})\n", FLAGS_op,
      iops, bandwidth, avglat, maxlat, minlat);
}

}  // namespace blockcache
}  // namespace dingofs
