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
 * Created Date: 2025-06-18
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_BENCHMARK_COLLECTOR_H_
#define DINGOFS_SRC_CACHE_BENCHMARK_COLLECTOR_H_

#include <bthread/execution_queue.h>
#include <bthread/execution_queue_inl.h>

#include <functional>

#include "common/status.h"

namespace dingofs {
namespace cache {

class Stat {
 public:
  Stat() = default;

  void Add(uint64_t bytes, uint64_t latency_us);

  // iops
  uint64_t IOPS(uint64_t interval_us) const;

  // bandwidth in MiB/s
  uint64_t Bandwidth(uint64_t interval_us) const;

  // latency in us
  uint64_t AvgLat() const;
  uint64_t MaxLat() const;
  uint64_t MinLat() const;

  uint64_t Count() const;

 private:
  uint64_t count_{0};
  uint64_t total_bytes_{0};
  uint64_t max_latency_us_{0};
  uint64_t min_latency_us_{0};
  uint64_t total_latency_us_{0};
};

class Collector {
 public:
  using Func = std::function<void(Stat* interval, Stat* total)>;

  Collector() = default;

  Status Start();
  Status Detory();

  void Submit(Func func);

 private:
  static int Executor(void* meta, bthread::TaskIterator<Func>& iter);

  Stat interval_;
  Stat total_;
  bthread::ExecutionQueueId<Func> queue_id_;
};

using CollectorSPtr = std::shared_ptr<Collector>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BENCHMARK_COLLECTOR_H_
