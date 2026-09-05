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

#ifndef DINGOFS_BLOCKCACHE_TOOLS_BENCHMARK_REPORTER_H_
#define DINGOFS_BLOCKCACHE_TOOLS_BENCHMARK_REPORTER_H_

#include <chrono>
#include <memory>

#include "blockcache/tools/benchmark/collector.h"
#include "common/status.h"
#include "utils/executor/executor.h"

namespace dingofs {
namespace blockcache {

class Reporter {
 public:
  explicit Reporter(CollectorSPtr collector);
  Reporter(const Reporter&) = delete;
  Reporter& operator=(const Reporter&) = delete;

  Status Start();
  void Shutdown();

 private:
  static constexpr uint64_t kReportIntervalSeconds = 3;

  void TickTok();

  void OnStart();
  void OnShow();
  void OnStop();

  uint64_t Drain(Stat* interval);
  void Show(const Stat& stat, uint64_t interval_us) const;
  uint64_t ElapsedUs() const;
  double Percent(const Stat* total) const;

  std::chrono::steady_clock::time_point start_;
  std::chrono::steady_clock::time_point last_;
  CollectorSPtr collector_;
  Stat total_;
  std::unique_ptr<Executor> executor_;
};

using ReporterSPtr = std::shared_ptr<Reporter>;

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_TOOLS_BENCHMARK_REPORTER_H_
