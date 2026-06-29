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

#include "cache/tools/bench/common/reporter.h"

#include <butil/time.h>
#include <glog/logging.h>

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <utility>

#include "cache/tools/bench/common/format.h"
#include "utils/executor/bthread/bthread_executor.h"

namespace dingofs {
namespace cache {
namespace bench {

namespace {

constexpr uint64_t kMiB = 1024 * 1024;

void PrintRow(const std::string& key, const std::string& value) {
  std::cout << "  " << std::left << std::setw(16) << key << value << '\n';
}

std::string Bar(uint64_t count, uint64_t max, uint64_t total) {
  constexpr int kWidth = 28;
  const int filled =
      max == 0 ? 0 : static_cast<int>((count * kWidth + max - 1) / max);
  std::ostringstream os;
  for (int i = 0; i < kWidth; ++i) {
    os << (i < filled ? "█" : " ");
  }
  const double pct = total == 0 ? 0.0 : (count * 100.0 / total);
  os << "  " << std::fixed << std::setprecision(1) << std::setw(5) << pct << "%";
  return os.str();
}

}  // namespace

ConsoleReporter::ConsoleReporter(std::string title, HeaderRows header,
                                 uint32_t report_interval_s, uint32_t warmup_s,
                                 Stats* stats)
    : title_(std::move(title)),
      header_(std::move(header)),
      report_interval_s_(report_interval_s),
      warmup_s_(warmup_s),
      stats_(CHECK_NOTNULL(stats)),
      executor_(std::make_unique<BthreadExecutor>()) {}

Status ConsoleReporter::Start() {
  if (!executor_->Start()) {
    return Status::Internal("start reporter timer failed");
  }
  start_us_ = butil::gettimeofday_us();
  last_us_ = 0;
  last_ops_ = 0;
  last_bytes_ = 0;
  running_.store(true, std::memory_order_release);
  PrintHeader();
  executor_->Schedule([this]() { Tick(); }, report_interval_s_ * 1000);
  return Status::OK();
}

void ConsoleReporter::Stop() {
  if (!running_.exchange(false, std::memory_order_acq_rel)) {
    return;
  }
  executor_->Stop();
  const auto elapsed_us = ElapsedUs();
  PrintSummary(stats_->Aggregate(), elapsed_us);
}

void ConsoleReporter::Tick() {
  if (!running_.load(std::memory_order_acquire)) {
    return;
  }
  PrintProgress(stats_->Aggregate(), ElapsedUs());
  if (running_.load(std::memory_order_acquire)) {
    executor_->Schedule([this]() { Tick(); }, report_interval_s_ * 1000);
  }
}

uint64_t ConsoleReporter::ElapsedUs() const {
  return butil::gettimeofday_us() - start_us_;
}

void ConsoleReporter::ResetBaseline() {
  const auto agg = stats_->Aggregate();
  last_us_ = ElapsedUs();
  last_ops_ = agg.ops;
  last_bytes_ = agg.bytes;
}

void ConsoleReporter::PrintHeader() const {
  std::cout << "\n" << title_ << "\n"
            << std::string(title_.size(), '=') << "\n";
  for (const auto& [key, value] : header_) {
    PrintRow(key, value);
  }
  std::cout << "\n"
            << std::left << std::setw(8) << "  time" << std::right
            << std::setw(11) << "IOPS" << std::setw(14) << "bandwidth"
            << std::setw(11) << "p50" << std::setw(11) << "p99" << std::setw(11)
            << "p999" << '\n';
}

void ConsoleReporter::PrintProgress(const Aggregated& agg,
                                    uint64_t elapsed_us) {
  const uint64_t interval_us = elapsed_us > last_us_ ? elapsed_us - last_us_ : 1;
  const uint64_t d_ops = agg.ops >= last_ops_ ? agg.ops - last_ops_ : agg.ops;
  const uint64_t d_bytes =
      agg.bytes >= last_bytes_ ? agg.bytes - last_bytes_ : agg.bytes;
  const double seconds = interval_us / 1000000.0;

  std::cout << "  " << std::left << std::setw(6)
            << FormatDuration(elapsed_us / 1000000.0) << std::right
            << std::setw(11) << FormatCount(d_ops / seconds) << std::setw(14)
            << FormatRate(d_bytes / seconds) << std::setw(11)
            << FormatLatencyUs(agg.hist.Percentile(0.50)) << std::setw(11)
            << FormatLatencyUs(agg.hist.Percentile(0.99)) << std::setw(11)
            << FormatLatencyUs(agg.hist.Percentile(0.999)) << '\n';

  last_us_ = elapsed_us;
  last_ops_ = agg.ops;
  last_bytes_ = agg.bytes;
}

void ConsoleReporter::PrintSummary(const Aggregated& agg,
                                   uint64_t elapsed_us) const {
  uint64_t measure_us = elapsed_us;
  if (warmup_s_ > 0) {
    const uint64_t warmup_us = static_cast<uint64_t>(warmup_s_) * 1000000ULL;
    measure_us = elapsed_us > warmup_us ? elapsed_us - warmup_us : elapsed_us;
  }
  const double seconds = measure_us == 0 ? 1.0 : measure_us / 1000000.0;

  std::cout << "\nSummary";
  if (warmup_s_ > 0) {
    std::cout << " (after " << warmup_s_ << "s warmup)";
  }
  std::cout << "\n-------\n";
  PrintRow("IOPS", FormatCount(agg.ops / seconds));
  PrintRow("bandwidth", FormatRate(agg.bytes / seconds));
  PrintRow("total I/O", FormatBytes(agg.bytes) + " in " +
                            std::to_string(agg.ops) + " ops");
  PrintRow("errors", std::to_string(agg.failed));

  std::cout << "\nLatency\n";
  PrintRow("min", FormatLatencyUs(agg.hist.Min()));
  PrintRow("mean", FormatLatencyUs(agg.hist.Mean()));
  PrintRow("p50", FormatLatencyUs(agg.hist.Percentile(0.50)));
  PrintRow("p90", FormatLatencyUs(agg.hist.Percentile(0.90)));
  PrintRow("p99", FormatLatencyUs(agg.hist.Percentile(0.99)));
  PrintRow("p999", FormatLatencyUs(agg.hist.Percentile(0.999)));
  PrintRow("max", FormatLatencyUs(agg.hist.Max()));

  uint64_t max_bucket = 0;
  for (size_t i = 0; i < kDistBuckets; ++i) {
    max_bucket = std::max(max_bucket, agg.dist[i]);
  }
  std::cout << "\nLatency distribution\n";
  for (size_t i = 0; i < kDistBuckets; ++i) {
    std::cout << "  " << std::left << std::setw(8) << kDistLabels[i]
              << Bar(agg.dist[i], max_bucket, agg.succeeded) << '\n';
  }
}

}  // namespace bench
}  // namespace cache
}  // namespace dingofs
