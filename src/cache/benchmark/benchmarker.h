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
 * Created Date: 2025-05-30
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_BENCHMARK_BENCHMARKER_H_
#define DINGOFS_SRC_CACHE_BENCHMARK_BENCHMARKER_H_

#include "blockaccess/block_accesser.h"
#include "cache/benchmark/reporter.h"
#include "cache/benchmark/worker.h"
#include "cache/blockcache/block_cache.h"

namespace dingofs {
namespace cache {

class Benchmarker {
 public:
  Benchmarker();

  Status Run();
  void Shutdown();

 private:
  Status Init();
  Status InitBlockCache();
  Status InitWrokers();

  Status Start();
  Status StartReporter();
  void StartWorkers();

  void Stop();
  void StopWorkers();
  void StopReporter();
  void StopBlockCache();

  blockaccess::BlockAccesserUPtr block_accesser_;
  BlockCacheSPtr block_cache_;
  TaskThreadPoolUPtr task_pool_;
  std::vector<WorkerSPtr> workers_;
  BthreadCountdownEventSPtr countdown_event_;
  ReporterSPtr reporter_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BENCHMARK_BENCHMARKER_H_
