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

#ifndef DINGOFS_CLIENT_VFS_COMPONENTS_WARMUP_METRIC_H_
#define DINGOFS_CLIENT_VFS_COMPONENTS_WARMUP_METRIC_H_

#include <bvar/bvar.h>

#include <cstdint>
#include <string>

namespace dingofs {
namespace client {
namespace vfs {

// Metrics for the warmup pipeline. There is one observation point per
// asynchronous handoff, so a stall can be located by finding the stage
// whose backlog keeps growing:
//
//   Submit -> ExecutionQueue -> Resolve executor -> dispatch_waiters_
//          -> PrefetchAsync (inflight blocks) -> BlockDone callback
//
// Gauges use a signed type because completion subtracts from the current
// value. Unsigned adders would wrap on a decrement.
struct WarmupMetric {
  inline static const std::string legacy_prefix = "dingofs_vfs";
  inline static const std::string prefix = "dingofs_vfs_warmup";

  // Warmup tasks currently in progress (registered but not finished).
  // +1 when HandleSubmit registers a task, -1 when FinishTask completes it.
  // Returns to 0 when no warmup is running. Keeps its pre-refactor name so
  // existing dashboards and alerts keep working.
  bvar::Adder<int64_t> inflight_warmup_tasks;

  // Prefetch blocks that were sent but whose callback has not been processed
  // yet. +1 in TryDispatchBlocks when a block is dispatched, -1 in
  // HandleBlockDone. Invariant: 0 <= value <=
  // FLAGS_vfs_warmup_max_inflight_blocks. Also keeps its pre-refactor name.
  bvar::Adder<int64_t> inflight_warmup_blocks;

  // Backlog of the warmup event queue: events already enqueued but not yet
  // picked up by the single handler thread. +1 right before an event is
  // enqueued (rolled back if the enqueue fails), -1 when the handler
  // dequeues it. A steadily growing value means the handler cannot keep up,
  // which delays BlockDone events and therefore block credit release.
  bvar::Adder<int64_t> event_queue_depth;

  // Tasks already handed to the resolve executor but still waiting in its
  // internal queue for a worker thread. +1 before Execute(), -1 when the
  // executor lambda starts (also rolled back if Execute() rejects the task).
  // Task submission is not rate-limited, so this is where backpressure
  // accumulates: a growing value means Resolve cannot keep up with
  // submissions.
  bvar::Adder<int64_t> resolve_queued_tasks;

  // Tasks currently running ResolveTask (directory walking and metadata
  // RPCs). +1 when the executor lambda starts, -1 when ResolveTask returns.
  // Under full load this equals the executor thread count; queued tasks
  // growing while this stays 0 means the executor is stuck or not started.
  bvar::Adder<int64_t> resolve_running_tasks;

  // Resolved tasks that still hold blocks waiting for dispatch credit, i.e.
  // the number of tasks in dispatch_waiters_. +1 when a task enters the
  // deque, -1 when it leaves. Growing while inflight_warmup_blocks stays at
  // the credit limit means prefetch callbacks come back too slowly to
  // consume resolved work.
  bvar::Adder<int64_t> dispatch_waiting_tasks;

  // Cumulative prefetch callbacks that succeeded (OK, or the block was
  // already cached = Exist). Success only means the cache node answered the
  // prefetch request; it does not prove the data is already readable from
  // cache.
  bvar::Adder<uint64_t> blocks_succeeded_total;

  // Cumulative prefetch callbacks that failed with any other status. This
  // is the primary warmup alerting signal: a rising rate usually means
  // cache node, RPC, or storage errors.
  bvar::Adder<uint64_t> blocks_failed_total;

  // Time taken by ResolveTask alone (walking directories and issuing
  // metadata RPCs to build the block list), in microseconds. Excludes
  // executor queueing and prefetch. Compare with task_latency to tell a
  // slow metadata phase apart from a slow dispatch/download phase.
  bvar::LatencyRecorder resolve_latency;

  // End-to-end warmup time per task: from HandleSubmit registering the task
  // until FinishTask completes it (all blocks answered), in microseconds.
  // Includes executor queueing, resolve, credit waiting, and prefetch. This
  // is the user-perceived warmup duration.
  bvar::LatencyRecorder task_latency;

  WarmupMetric()
      : inflight_warmup_tasks(legacy_prefix, "inflight_warmup_tasks"),
        inflight_warmup_blocks(legacy_prefix, "inflight_warmup_blocks"),
        event_queue_depth(prefix, "event_queue_depth"),
        resolve_queued_tasks(prefix, "resolve_queued_tasks"),
        resolve_running_tasks(prefix, "resolve_running_tasks"),
        dispatch_waiting_tasks(prefix, "dispatch_waiting_tasks"),
        blocks_succeeded_total(prefix, "blocks_succeeded_total"),
        blocks_failed_total(prefix, "blocks_failed_total"),
        resolve_latency(prefix, "resolve_latency"),
        task_latency(prefix, "task_latency") {}
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_COMPONENTS_WARMUP_METRIC_H_
