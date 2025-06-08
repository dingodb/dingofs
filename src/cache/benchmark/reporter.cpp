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

#include "cache/benchmark/reporter.h"

#include <memory>

#include "base/timer/timer_impl.h"
#include "cache/common/common.h"
#include "cache/config/config.h"

namespace dingofs {
namespace cache {

extern BenchmarkOption* g_option;

Reporter::Reporter() : timer_(std::make_unique<base::timer::TimerImpl>()) {}

Status Reporter::Start() {
  bthread::ExecutionQueueOptions queue_options;
  queue_options.use_pthread = true;
  int rc = bthread::execution_queue_start(&queue_id_, &queue_options, Aggregate,
                                          this);
  if (rc != 0) {
    return Status::Internal("stop execution queue failed");
  }

  CHECK(timer_->Start());
  timer_->Add([this]() { ReportOnce(); }, 3 * 1000);
  return Status::OK();
}

void Reporter::Submit(Record record) {
  // LOG(INFO) << "submit: " << record.bytes << ": " << record.latency_s;
  CHECK_EQ(0, bthread::execution_queue_execute(queue_id_, record));
}

int Reporter::Aggregate(void* meta, bthread::TaskIterator<Record>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  Reporter* r = static_cast<Reporter*>(meta);
  for (; iter; iter++) {
    auto& record = *iter;
    r->Add(record);
  }
  return 0;
}

void Reporter::Add(const Record& record) {
  std::unique_lock<BthreadMutex> lk(mutex_);
  agg_.Add(record.bytes, record.latency_s);
}

// async_put:   1000 op/s   4090 MB/s  lat(0.000001 0.000001 0.000001)
//       put:   1000 op/s   4090 MB/s  lat(0.000001 0.000001 0.000001)
void Reporter::ReportOnce() {
  AggRecord out;
  {
    std::unique_lock<BthreadMutex> lk(mutex_);
    out = agg_;
    agg_ = AggRecord();
  }

  uint64_t iops = out.count / 3;
  uint64_t bw = out.total_bytes / 3 / kMiB;
  double avg_latency = (out.count == 0) ? 0 : out.total_latency_s / out.count;

  std::cout << absl::StrFormat(
                   "%s: %6lld op/s  %5lld MB/s  lat(%.6lf %.6lf %.6lf)",
                   g_option->op, iops, bw, avg_latency, out.max_latency_s,
                   out.min_latency_s)
            << std::endl;

  timer_->Add([this]() { ReportOnce(); }, 3 * 1000);
}

}  // namespace cache
}  // namespace dingofs
