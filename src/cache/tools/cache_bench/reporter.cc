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

#include "cache/tools/cache_bench/reporter.h"

#include <glog/logging.h>

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>

#include "common/options/cache.h"
#include "utils/executor/bthread/bthread_executor.h"

namespace dingofs {
namespace cache {
namespace {

constexpr uint64_t kMiB = 1024 * 1024;

std::string YesNo(bool value) { return value ? "yes" : "no"; }

std::string FormatBandwidth(double mib_per_s) {
  const auto bytes_per_s =
      static_cast<uint64_t>(std::max(0.0, mib_per_s) * kMiB);
  return FormatBytes(bytes_per_s) + "/s";
}

std::string FormatLatency(uint64_t latency_us) {
  std::ostringstream os;
  if (latency_us < 1000) {
    os << latency_us << " us";
  } else if (latency_us < 1000 * 1000) {
    os << std::fixed << std::setprecision(2) << latency_us / 1000.0 << " ms";
  } else {
    os << std::fixed << std::setprecision(3) << latency_us / 1000000.0 << " s";
  }
  return os.str();
}

void PrintRow(const std::string& key, const std::string& value) {
  std::cout << "  " << std::left << std::setw(18) << key << value << '\n';
}

}  // namespace

ConsoleReporter::ConsoleReporter(const Options& options, Stats* stats)
    : options_(options),
      stats_(CHECK_NOTNULL(stats)),
      executor_(std::make_unique<BthreadExecutor>()) {}

Status ConsoleReporter::Start() {
  if (!executor_->Start()) {
    return Status::Internal("start reporter timer failed");
  }

  timer_.start();
  last_report_us_ = 0;
  running_.store(true, std::memory_order_release);
  PrintHeader();
  executor_->Schedule([this]() { Tick(); }, options_.report_interval_s * 1000);
  return Status::OK();
}

void ConsoleReporter::Stop() {
  if (!running_.exchange(false, std::memory_order_acq_rel)) {
    return;
  }

  timer_.stop();
  executor_->Stop();

  const auto elapsed_us = ElapsedUs();
  auto final_interval = stats_->TakeInterval();
  if (!final_interval.Empty()) {
    PrintProgress(final_interval, IntervalUs(elapsed_us), elapsed_us);
  }
  PrintSummary(elapsed_us);
}

void ConsoleReporter::Tick() {
  if (!running_.load(std::memory_order_acquire)) {
    return;
  }

  const auto elapsed_us = ElapsedUs();
  const auto interval_us = IntervalUs(elapsed_us);
  auto interval = stats_->TakeInterval();
  PrintProgress(interval, interval_us, elapsed_us);
  last_report_us_ = elapsed_us;

  if (running_.load(std::memory_order_acquire)) {
    executor_->Schedule([this]() { Tick(); },
                        options_.report_interval_s * 1000);
  }
}

uint64_t ConsoleReporter::ElapsedUs() const {
  return static_cast<uint64_t>(timer_.u_elapsed());
}

uint64_t ConsoleReporter::IntervalUs(uint64_t elapsed_us) const {
  if (elapsed_us <= last_report_us_) {
    return options_.report_interval_s * 1000000ULL;
  }
  return elapsed_us - last_report_us_;
}

void ConsoleReporter::PrintHeader() const {
  std::cout << "\nDingoFS Cache Bench\n";
  std::cout << "===================\n";
  PrintRow("operation", OperationName(options_.operation));
  PrintRow("mds_addrs", FLAGS_mds_addrs);
  PrintRow("fsid", std::to_string(options_.fs_id));
  PrintRow("threads", std::to_string(options_.threads));
  PrintRow("blocks/thread", std::to_string(options_.blocks_per_worker));
  PrintRow("block size", FormatBytes(options_.block_size));
  if (options_.operation == OperationType::kPut) {
    PrintRow("writeback", YesNo(options_.writeback));
  } else {
    PrintRow("range offset", FormatBytes(options_.range_offset));
    PrintRow("range length", FormatBytes(options_.range_length));
    PrintRow("retrieve storage", YesNo(options_.retrieve_storage));
  }
  PrintRow("total ops", std::to_string(options_.TotalOps()));
  PrintRow("total payload", FormatBytes(options_.TotalBytes()));

  std::cout << "\nProgress\n";
  std::cout << "  " << std::left << std::setw(10) << "elapsed" << std::right
            << std::setw(9) << "done" << std::setw(13) << "ops/s"
            << std::setw(16) << "bandwidth" << std::setw(13) << "avg lat"
            << std::setw(13) << "max lat" << std::setw(9) << "errors" << '\n';
}

void ConsoleReporter::PrintProgress(const StatsSnapshot& snapshot,
                                    uint64_t interval_us,
                                    uint64_t elapsed_us) const {
  const auto total = stats_->Total();
  const double percent = options_.TotalOps() == 0
                             ? 100.0
                             : total.operations * 100.0 / options_.TotalOps();

  std::ostringstream done;
  done << std::fixed << std::setprecision(1) << std::min(percent, 100.0) << '%';

  std::ostringstream ops;
  ops << std::fixed << std::setprecision(1) << snapshot.OpsPerSec(interval_us);

  std::cout << "  " << std::left << std::setw(10)
            << FormatDuration(elapsed_us / 1000000.0) << std::right
            << std::setw(9) << done.str() << std::setw(13) << ops.str()
            << std::setw(16) << FormatBandwidth(snapshot.MiBPerSec(interval_us))
            << std::setw(13) << FormatLatency(snapshot.AvgLatencyUs())
            << std::setw(13) << FormatLatency(snapshot.max_latency_us)
            << std::setw(9) << total.failed << '\n';
}

void ConsoleReporter::PrintSummary(uint64_t elapsed_us) const {
  const auto total = stats_->Total();

  std::cout << "\nSummary\n";
  std::cout << "-------\n";
  PrintRow("elapsed", FormatDuration(elapsed_us / 1000000.0));
  PrintRow("operations", std::to_string(total.operations) + " / " +
                             std::to_string(options_.TotalOps()));
  PrintRow("success", std::to_string(total.succeeded));
  PrintRow("failed", std::to_string(total.failed));
  PrintRow("payload", FormatBytes(total.bytes));
  PrintRow("throughput", FormatBandwidth(total.MiBPerSec(elapsed_us)));
  PrintRow("ops/s", [&]() {
    std::ostringstream os;
    os << std::fixed << std::setprecision(1) << total.OpsPerSec(elapsed_us);
    return os.str();
  }());
  PrintRow("lat avg", FormatLatency(total.AvgLatencyUs()));
  PrintRow("lat min", FormatLatency(total.min_latency_us));
  PrintRow("lat max", FormatLatency(total.max_latency_us));
}

}  // namespace cache
}  // namespace dingofs
