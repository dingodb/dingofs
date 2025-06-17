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

#ifndef DINGOFS_SRC_CACHE_BENCHMARK_REPORTER_H_
#define DINGOFS_SRC_CACHE_BENCHMARK_REPORTER_H_

#include "cache/benchmark/collector.h"
#include "utils/executor/timer.h"

namespace dingofs {
namespace cache {

class Reporter {
 public:
  explicit Reporter(CollectorSPtr collector);

  Status Start();
  void Shutdown();

 private:
  constexpr static uint64_t kReportIntervalSeconds = 3;

  void TickTok();

  void OnStart(Stat* stat, Stat* total);
  void OnShow(Stat* stat, Stat* total);
  void OnStop(Stat* stat, Stat* total);

  butil::Timer g_timer_;
  CollectorSPtr collector_;
  TimerUPtr timer_;
};

using ReporterSPtr = std::shared_ptr<Reporter>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BENCHMARK_REPORTER_H_
