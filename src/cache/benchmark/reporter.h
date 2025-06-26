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

#include <bthread/execution_queue.h>
#include <bthread/execution_queue_inl.h>
#include <butil/time.h>
#include <memory>

#include "cache/common/common.h"
#include "cache/config/config.h"
#include "utils/executor/executor.h"

namespace dingofs {
namespace cache {

enum class EventType : uint8_t {
  kOnStart = 0,
  kAddStat = 1,
  kReportStat = 2,
  kOnStop = 3,
};

struct Event {
  Event() = default;

  Event(EventType type, uint64_t bytes = 0, double latency_s = 0)
      : type(type), bytes(bytes), latency_s(latency_s) {}

  EventType type;
  uint64_t bytes{0};
  double latency_s{0};
};

class Statistics {  // Per 3 sconds
 public:
  Statistics() = default;

  void Add(uint64_t bytes, double latency_s) {
    max_latency_s_ = std::max(max_latency_s_, latency_s);
    if (min_latency_s_ == 0 || latency_s < min_latency_s_) {
      min_latency_s_ = latency_s;
    }

    count_++;
    total_bytes_ += bytes;
    total_latency_s += latency_s;
  }

  uint64_t IOPS(uint32_t interval_s = FLAGS_stat_interval_s) const {
    return count_ / interval_s;
  }

  uint64_t Bandwidth(uint32_t interval_s = FLAGS_stat_interval_s) const {
    return total_bytes_ * 1.0 / interval_s / kMiB;
  }

  double TotalLatency() const { return total_latency_s; }

  double AvgLatency() const {
    if (count_ == 0) {
      return 0;
    }
    return total_latency_s / count_;
  }

  double MaxLatency() const { return max_latency_s_; }

  double MinLatency() const { return min_latency_s_; }

  void Reset() {
    count_ = 0;
    total_bytes_ = 0;
    max_latency_s_ = 0;
    min_latency_s_ = 0;
    total_latency_s = 0;
  }

 private:
  uint64_t count_{0};
  uint64_t total_bytes_{0};
  double max_latency_s_{0};
  double min_latency_s_{0};
  double total_latency_s{0};
};

class Reporter {
 public:
  Reporter();

  Status Start();
  Status Stop();

  void Submit(Event event);

 private:
  static int HandleEvent(void* meta, bthread::TaskIterator<Event>& iter);

  void OnStart();
  void AddStat(Event event);
  void ReportStat();
  void OnStop();

  void TickTok();

  butil::Timer btimer_;
  Statistics interval_stat_;
  Statistics summary_stat_;
  bthread::ExecutionQueueId<Event> queue_id_;
  std::unique_ptr<Executor> executor_;
};

using ReporterSPtr = std::shared_ptr<Reporter>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BENCHMARK_REPORTER_H_
