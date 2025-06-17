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

#include "cache/benchmark/collector.h"
#include "cache/benchmark/factory.h"
#include "cache/common/type.h"

namespace dingofs {
namespace cache {

class Worker {
 public:
  Worker(uint64_t idx, TaskFactorySPtr factory, CollectorSPtr collector);

  void Start();
  void Shutdown();

 private:
  void ExecAllTasks();
  void ExecTask(std::function<void()> task);

  uint64_t idx_;
  TaskFactorySPtr factory_;
  CollectorSPtr collector_;
  BthreadCountdownEvent countdown_;
};

using WorkerUPtr = std::unique_ptr<Worker>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BENCHMARK_WORKER_H_
