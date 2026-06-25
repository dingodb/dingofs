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

#ifndef DINGOFS_SRC_CACHE_TOOLS_CACHE_BENCH_REPORTER_H_
#define DINGOFS_SRC_CACHE_TOOLS_CACHE_BENCH_REPORTER_H_

#include <butil/time.h>

#include <atomic>
#include <cstdint>
#include <memory>

#include "cache/tools/cache_bench/options.h"
#include "cache/tools/cache_bench/stats.h"
#include "common/status.h"
#include "utils/executor/executor.h"

namespace dingofs {
namespace cache {

class ConsoleReporter {
 public:
  ConsoleReporter(const Options& options, Stats* stats);

  Status Start();
  void Stop();

 private:
  void Tick();
  void PrintHeader() const;
  void PrintProgress(const StatsSnapshot& snapshot, uint64_t interval_us,
                     uint64_t elapsed_us) const;
  void PrintSummary(uint64_t elapsed_us) const;

  uint64_t ElapsedUs() const;
  uint64_t IntervalUs(uint64_t elapsed_us) const;

  Options options_;
  Stats* stats_;
  std::unique_ptr<Executor> executor_;
  butil::Timer timer_;
  uint64_t last_report_us_{0};
  std::atomic_bool running_{false};
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_TOOLS_CACHE_BENCH_REPORTER_H_
