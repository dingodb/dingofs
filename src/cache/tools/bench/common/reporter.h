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

#ifndef DINGOFS_SRC_CACHE_TOOLS_BENCH_COMMON_REPORTER_H_
#define DINGOFS_SRC_CACHE_TOOLS_BENCH_COMMON_REPORTER_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "cache/tools/bench/common/stats.h"
#include "common/status.h"
#include "utils/executor/executor.h"

namespace dingofs {
namespace cache {
namespace bench {

// Shared console reporter for the throughput/latency subcommands (aio/fs/store/
// client). The subcommand supplies a title and its own header rows; the
// progress line, summary and latency distribution are generic.
class ConsoleReporter {
 public:
  using HeaderRows = std::vector<std::pair<std::string, std::string>>;

  ConsoleReporter(std::string title, HeaderRows header,
                  uint32_t report_interval_s, uint32_t warmup_s, Stats* stats);

  Status Start();
  void Stop();
  void ResetBaseline();  // re-baseline interval throughput after a warmup reset

 private:
  void Tick();
  void PrintHeader() const;
  void PrintProgress(const Aggregated& agg, uint64_t elapsed_us);
  void PrintSummary(const Aggregated& agg, uint64_t elapsed_us) const;
  uint64_t ElapsedUs() const;

  std::string title_;
  HeaderRows header_;
  uint32_t report_interval_s_;
  uint32_t warmup_s_;
  Stats* stats_;
  std::unique_ptr<Executor> executor_;

  uint64_t start_us_{0};
  std::atomic_bool running_{false};
  uint64_t last_us_{0};
  uint64_t last_ops_{0};
  uint64_t last_bytes_{0};
};

}  // namespace bench
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_TOOLS_BENCH_COMMON_REPORTER_H_
