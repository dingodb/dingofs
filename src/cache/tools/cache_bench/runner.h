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

#ifndef DINGOFS_SRC_CACHE_TOOLS_CACHE_BENCH_RUNNER_H_
#define DINGOFS_SRC_CACHE_TOOLS_CACHE_BENCH_RUNNER_H_

#include <memory>

#include "cache/api/block_cache.h"
#include "cache/common/mds_client.h"
#include "cache/tools/cache_bench/operation.h"
#include "cache/tools/cache_bench/options.h"
#include "cache/tools/cache_bench/reporter.h"
#include "cache/tools/cache_bench/stats.h"
#include "common/blockaccess/block_accesser.h"
#include "utils/concurrent/task_thread_pool.h"

namespace dingofs {
namespace cache {

class Runner {
 public:
  explicit Runner(Options options);
  ~Runner();

  Status Run();

 private:
  Status Init();
  Status InitMdsClient();
  Status InitBlockAccesser();
  Status InitBlockCache();
  Status InitWorkers();

  void RunWorkers();
  void RunWorker(uint32_t worker_id);
  void Shutdown();

  Options options_;
  Stats stats_;
  MDSClientSPtr mds_client_;
  blockaccess::BlockAccesserUPtr block_accesser_;
  BlockCacheSPtr block_cache_;
  OperationUPtr operation_;
  std::unique_ptr<ConsoleReporter> reporter_;
  utils::TaskThreadPoolUPtr workers_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_TOOLS_CACHE_BENCH_RUNNER_H_
