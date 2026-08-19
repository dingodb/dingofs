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

#ifndef DINGOFS_BLOCKCACHE_TOOLS_BENCHMARK_BENCHMARKER_H_
#define DINGOFS_BLOCKCACHE_TOOLS_BENCHMARK_BENCHMARKER_H_

#include <memory>
#include <vector>

#include "blockcache/api/cache.h"
#include "blockcache/common/mds_client.h"
#include "blockcache/object/client.h"
#include "blockcache/tools/benchmark/collector.h"
#include "blockcache/tools/benchmark/factory.h"
#include "blockcache/tools/benchmark/reporter.h"
#include "blockcache/tools/benchmark/worker.h"
#include "utils/concurrent/task_thread_pool.h"

namespace dingofs {
namespace blockcache {

class Benchmarker {
 public:
  Benchmarker();

  Status Start();

  void RunUntilFinish();

 private:
  Status InitAll();
  Status InitMdsClient();
  Status InitStorage();
  Status InitBlockCache();
  Status InitBuffers();
  Status InitCollector();
  void InitFactory();
  void InitWorkers();

  void StartAll();
  void StartReporter();
  void StartWorkers();

  void StopAll();
  void StopWorkers();
  void StopReporter();
  void StopCollector();
  void StopBlockCache();

  MDSClientUPtr mds_client_;
  StorageClientUPtr storage_client_;
  std::unique_ptr<BlockCacheImpl> block_cache_;
  char* buffers_{nullptr};
  CollectorSPtr collector_;
  ReporterSPtr reporter_;
  TaskFactorySPtr factory_;
  std::vector<WorkerUPtr> workers_;
  std::unique_ptr<utils::TaskThreadPool<>> thread_pool_;
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_TOOLS_BENCHMARK_BENCHMARKER_H_
