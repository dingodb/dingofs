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
#include <sys/types.h>

#include <cstdint>
#include <memory>
#include <mutex>

#include "base/timer/timer.h"
#include "cache/common/type.h"

namespace dingofs {
namespace cache {

struct Record {
  Record(uint64_t bytes, double latency_s)
      : bytes(bytes), latency_s(latency_s) {}

  uint64_t bytes{0};
  double latency_s{0};
};

struct AggRecord {
  AggRecord() = default;

  void Add(uint64_t bytes, double latency_s) {
    count++;
    total_bytes += bytes;
    max_latency_s = std::max(max_latency_s, latency_s);
    min_latency_s = std::min(min_latency_s, latency_s);
    total_latency_s += latency_s;
  }

  uint64_t count{0};
  uint64_t total_bytes{0};
  double max_latency_s{0};
  double min_latency_s{99999999999};
  double total_latency_s{0};
};

class Reporter {
 public:
  Reporter();

  Status Start();

  void Submit(Record record);

 private:
  static int Aggregate(void* meta, bthread::TaskIterator<Record>& iter);
  void Add(const Record& record);
  void ReportOnce();

  BthreadMutex mutex_;
  AggRecord agg_;
  bthread::ExecutionQueueId<Record> queue_id_;
  std::unique_ptr<base::timer::Timer> timer_;
};

using ReporterSPtr = std::shared_ptr<Reporter>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BENCHMARK_REPORTER_H_
