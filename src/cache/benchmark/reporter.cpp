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

#include <absl/strings/str_format.h>

#include "cache/common/common.h"
#include "cache/config/benchmark.h"
#include "cache/config/config.h"
#include "utils/executor/timer_impl.h"

namespace dingofs {
namespace cache {

Reporter::Reporter() : queue_id_({0}), timer_(std::make_unique<TimerImpl>()) {
  btimer_.start();
}

Status Reporter::Start() {
  bthread::ExecutionQueueOptions queue_options;
  queue_options.use_pthread = true;
  int rc = bthread::execution_queue_start(&queue_id_, &queue_options,
                                          HandleEvent, this);
  if (rc != 0) {
    return Status::Internal("stop execution queue failed");
  }

  Submit(Event(EventType::kOnStart));

  CHECK(timer_->Start());
  timer_->Add([this]() { TickTok(); }, FLAGS_stat_interval_s * 1000);

  return Status::OK();
}

Status Reporter::Stop() {
  timer_->Stop();
  Submit(Event(EventType::kOnStop));

  int rc = bthread::execution_queue_stop(queue_id_);
  if (rc != 0) {
    return Status::Internal("stop execution queue failed");
  }

  rc = bthread::execution_queue_join(queue_id_);
  if (rc != 0) {
    return Status::Internal("join execution queue failed");
  }

  return Status::OK();
}

void Reporter::TickTok() {
  Submit(Event(EventType::kReportStat));
  timer_->Add([this]() { TickTok(); }, FLAGS_stat_interval_s * 1000);
}

void Reporter::Submit(Event event) {
  CHECK_EQ(0, bthread::execution_queue_execute(queue_id_, event));
}

int Reporter::HandleEvent(void* meta, bthread::TaskIterator<Event>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  Reporter* r = static_cast<Reporter*>(meta);
  for (; iter; iter++) {
    auto& event = *iter;
    switch (event.type) {
      case EventType::kOnStart:
        r->OnStart();
        break;

      case EventType::kAddStat:
        r->AddStat(event);
        break;

      case EventType::kReportStat:
        r->ReportStat();
        break;

      case EventType::kOnStop:
        r->OnStop();
        break;

      default:
        CHECK(false) << "Unknown event type: " << static_cast<int>(event.type);
    }
  }

  return 0;
}

void Reporter::OnStart() {
  std::cout << absl::StrFormat("op=%s threads=%d blksize=%lu blocks=%lu\n",
                               FLAGS_op, FLAGS_threads, FLAGS_op_blksize,
                               FLAGS_op_blocks);
}

void Reporter::AddStat(Event event) {
  interval_stat_.Add(event.bytes, event.latency_s);
  summary_stat_.Add(event.bytes, event.latency_s);
}

// async_put:   1000 op/s   4090 MB/s  lat(0.000001 0.000001 0.000001)
//       put:   1000 op/s   4090 MB/s  lat(0.000001 0.000001 0.000001)
void Reporter::ReportStat() {
  auto stat = interval_stat_;
  std::cout << absl::StrFormat(
      "%9s: %6lld op/s  %5lld MB/s  lat(%.6lf %.6lf %.6lf)\n", FLAGS_op,
      stat.IOPS(), stat.Bandwidth(), stat.AvgLatency(), stat.MaxLatency(),
      stat.MinLatency());

  interval_stat_.Reset();
}

//  avg(put):   1000 op/s   4090 MB/s  lat(0.000001 0.000001 0.000001)
void Reporter::OnStop() {
  btimer_.stop();
  auto elapsed_s = btimer_.s_elapsed();

  auto stat = summary_stat_;
  std::cout << absl::StrFormat(
      "%9s: %6lld op/s  %5lld MB/s  lat(%.6lf %.6lf %.6lf)\n",
      absl::StrFormat("avg(%s)", FLAGS_op), stat.IOPS(elapsed_s),
      stat.Bandwidth(elapsed_s), stat.AvgLatency(), stat.MaxLatency(),
      stat.MinLatency());
}

}  // namespace cache
}  // namespace dingofs
