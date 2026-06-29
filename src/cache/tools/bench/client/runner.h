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

#ifndef DINGOFS_SRC_CACHE_TOOLS_BENCH_CLIENT_RUNNER_H_
#define DINGOFS_SRC_CACHE_TOOLS_BENCH_CLIENT_RUNNER_H_

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <random>
#include <vector>

#include "cache/api/block_cache.h"
#include "cache/common/mds_client.h"
#include "cache/tools/bench/client/key_generator.h"
#include "cache/tools/bench/client/operation.h"
#include "cache/tools/bench/client/options.h"
#include "cache/tools/bench/common/reporter.h"
#include "cache/tools/bench/common/stats.h"
#include "common/blockaccess/block_accesser.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace bench {
namespace client {

// Open-loop generators against TierBlockCache (the full client stack). With
// qps>0, latency is measured from each op's scheduled time (coordinated-omission
// safe); qps==0 runs as fast as the inflight cap allows.
class Runner {
 public:
  explicit Runner(Options options);
  ~Runner();

  Status Run();

 private:
  struct alignas(64) Worker {
    bthread::Mutex mutex;
    bthread::ConditionVariable cond;
    uint64_t inflight{0};
    std::mt19937_64 rng;
    uint64_t next_schedule_us{0};
  };

  struct GenArg {
    Runner* self;
    uint32_t id;
  };

  Status Init();
  Status InitMdsClient();
  Status InitBlockAccesser();
  Status InitBlockCache();

  void Generate(uint32_t id);
  void OnComplete(uint32_t id, uint64_t schedule_us, uint64_t bytes,
                  const Status& status);
  static void* GenEntry(void* arg);

  void Shutdown();

  Options options_;
  std::unique_ptr<Stats> stats_;
  MDSClientSPtr mds_client_;
  blockaccess::BlockAccesserUPtr block_accesser_;
  BlockCacheSPtr block_cache_;
  OperationUPtr operation_;
  std::unique_ptr<KeyGenerator> key_generator_;
  std::unique_ptr<ConsoleReporter> reporter_;
  std::vector<std::unique_ptr<Worker>> workers_;

  uint64_t start_us_{0};
  uint64_t deadline_us_{0};
  uint32_t inflight_per_worker_{0};
  double schedule_interval_us_{0};
  std::atomic<uint64_t> issued_{0};
};

}  // namespace client
}  // namespace bench
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_TOOLS_BENCH_CLIENT_RUNNER_H_
