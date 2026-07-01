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

#ifndef DINGOFS_SRC_CACHE_TOOLS_BENCH_AIO_RUNNER_H_
#define DINGOFS_SRC_CACHE_TOOLS_BENCH_AIO_RUNNER_H_

#include <sys/uio.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "cache/local/aio_queue.h"
#include "cache/tools/bench/aio/options.h"
#include "cache/tools/bench/common/profiler.h"
#include "cache/tools/bench/common/reporter.h"
#include "cache/tools/bench/common/stats.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace bench {
namespace aio {

// --threads closed-loop worker bthreads, each keeping exactly one i/o in flight
// (Submit + Wait), so the offered concurrency (in-flight i/o count) is
// --threads. --iodepth only sizes the AioQueue/io_uring ring (FLAGS_iodepth),
// like the production cache uses it.
class Runner {
 public:
  explicit Runner(Options options);
  ~Runner();

  Status Run();

 private:
  struct GenArg {
    Runner* self;
    uint32_t id;
  };

  Status Init();
  Status AllocBuffers();
  Status OpenFiles();
  Status PrepFiles();

  void RunWorker(uint32_t worker);
  static void* GenEntry(void* arg);

  std::string FilePath(uint32_t index) const;
  void Shutdown();

  Options options_;
  std::unique_ptr<Stats> stats_;
  std::unique_ptr<ConsoleReporter> reporter_;
  AioQueueUPtr queue_;

  std::vector<int> fds_;
  std::vector<char*> buffers_;
  std::vector<iovec> fixed_iovecs_;

  uint64_t deadline_us_{0};
  std::atomic<bool> stop_{false};
  std::atomic<uint64_t> bytes_issued_{0};
  // Breakdown of one closed-loop op: Submit() call vs blocking in Wait().
  std::atomic<uint64_t> submit_sum_us_{0};
  std::atomic<uint64_t> wait_sum_us_{0};
  std::atomic<uint64_t> op_count_{0};
  Profiler profiler_;
};

}  // namespace aio
}  // namespace bench
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_TOOLS_BENCH_AIO_RUNNER_H_
