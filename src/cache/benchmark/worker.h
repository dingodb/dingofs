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

#ifndef DINGOFS_SRC_CACHE_BENCHMARK_WORKER_H_
#define DINGOFS_SRC_CACHE_BENCHMARK_WORKER_H_

#include <bthread/countdown_event.h>

#include "cache/benchmark/collector.h"
#include "cache/benchmark/factory.h"

namespace dingofs {
namespace cache {

class Worker {
 public:
  // `warmed` is signaled once this worker has finished init + warmup; `go` is
  // waited on before the measured phase so the wall clock (started by the
  // benchmarker after all workers are warm) excludes pool registration and
  // cold-start effects.
  Worker(uint64_t idx, TaskFactorySPtr factory, CollectorSPtr collector,
         bthread::CountdownEvent* warmed, bthread::CountdownEvent* go);

  void Start();
  void Shutdown();

 private:
  void RunWarmup();
  void ExecAllTasks();
  void ExecTask(Task task);

  uint64_t idx_;
  TaskFactorySPtr factory_;
  CollectorSPtr collector_;
  TaskContext context_;
  bthread::CountdownEvent finished_;
  bthread::CountdownEvent* warmed_;
  bthread::CountdownEvent* go_;
};

using WorkerUPtr = std::unique_ptr<Worker>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BENCHMARK_WORKER_H_
