
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
 * Created Date: 2025-06-17
 * Author: Jingli Chen (Wine93)
 */

#include "cache/benchmark/collector.h"

#include <algorithm>
#include <cmath>

#include "common/const.h"

namespace dingofs {
namespace cache {

void Stat::Add(uint64_t bytes, uint64_t latency_us, bool ok) {
  count_++;
  if (!ok) {
    fail_count_++;
    return;
  }

  max_latency_us_ = std::max(max_latency_us_, latency_us);
  if (min_latency_us_ == 0 || latency_us < min_latency_us_) {
    min_latency_us_ = latency_us;
  }

  success_count_++;
  total_bytes_ += bytes;
  total_latency_us_ += latency_us;
  latencies_us_.push_back(latency_us);
}

double Stat::IOPS(uint64_t interval_us) const {
  if (interval_us == 0) {
    return 0;
  }
  return success_count_ / (interval_us * 1.0 / 1e6);
}

double Stat::Bandwidth(uint64_t interval_us) const {
  if (interval_us == 0) {
    return 0;
  }
  return total_bytes_ * 1.0 / (interval_us * 1.0 / 1e6) / kMiB;
}

uint64_t Stat::AvgLat() const {
  if (success_count_ == 0) {
    return 0;
  }
  return total_latency_us_ / success_count_;
}

uint64_t Stat::MaxLat() const { return max_latency_us_; }

uint64_t Stat::MinLat() const { return min_latency_us_; }

uint64_t Stat::PercentileLat(double percentile) const {
  if (latencies_us_.empty()) {
    return 0;
  }
  auto latencies = latencies_us_;
  std::sort(latencies.begin(), latencies.end());
  auto index = static_cast<size_t>(
      std::ceil(percentile / 100.0 * latencies.size()) - 1);
  index = std::min(index, latencies.size() - 1);
  return latencies[index];
}

uint64_t Stat::Count() const { return count_; }

uint64_t Stat::SuccessCount() const { return success_count_; }

uint64_t Stat::FailCount() const { return fail_count_; }

uint64_t Stat::TotalBytes() const { return total_bytes_; }

Status Collector::Start() {
  bthread::ExecutionQueueOptions queue_options;
  queue_options.use_pthread = true;
  int rc = bthread::execution_queue_start(&queue_id_, &queue_options, Executor,
                                          this);
  if (rc != 0) {
    return Status::Internal("start collector execution queue failed");
  }

  return Status::OK();
}

Status Collector::Detory() {
  int rc = bthread::execution_queue_stop(queue_id_);
  if (rc != 0) {
    return Status::Internal("stop collector execution queue failed");
  }

  rc = bthread::execution_queue_join(queue_id_);
  if (rc != 0) {
    return Status::Internal("join collector execution queue failed");
  }

  return Status::OK();
}

void Collector::Submit(Func func) {
  CHECK_EQ(0, bthread::execution_queue_execute(queue_id_, func));
}

int Collector::Executor(void* meta, bthread::TaskIterator<Func>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  Collector* c = static_cast<Collector*>(meta);
  for (; iter; iter++) {
    auto& func = *iter;
    func(&c->interval_, &c->total_);
  }
  return 0;
}

}  // namespace cache
}  // namespace dingofs
