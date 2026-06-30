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

#ifndef DINGOFS_SRC_CACHE_TOOLS_BENCH_STORE_RUNNER_H_
#define DINGOFS_SRC_CACHE_TOOLS_BENCH_STORE_RUNNER_H_

#include <atomic>
#include <cstdint>
#include <memory>

#include "cache/tools/bench/common/profiler.h"
#include "cache/tools/bench/common/reporter.h"
#include "cache/tools/bench/common/stats.h"
#include "cache/tools/bench/store/options.h"
#include "common/block/block_handle.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace bench {
namespace store {

// Uniform write/read over a cache layer: Cache/Load for a CacheStore, Cache/
// Range for a BlockCache. Fill makes a block readable for the prep phase.
class BenchTarget {
 public:
  virtual ~BenchTarget() = default;
  virtual Status Start() = 0;
  virtual void Shutdown() = 0;
  virtual Status Write(const BlockHandle& handle, IOBuffer payload) = 0;
  virtual Status Read(const BlockHandle& handle, off_t offset, size_t length,
                      IOBuffer* buffer) = 0;
  virtual Status Fill(const BlockHandle& handle, IOBuffer payload) = 0;
};

using BenchTargetUPtr = std::unique_ptr<BenchTarget>;

BenchTargetUPtr NewTarget(const Options& options);

class Runner {
 public:
  explicit Runner(Options options);
  ~Runner();

  Status Run();

 private:
  Status Init();
  Status PrepBlocks();
  void RunWorker(uint32_t id);
  BlockHandle MakeHandle(uint64_t key) const;
  void Shutdown();

  Options options_;
  std::unique_ptr<Stats> stats_;
  std::unique_ptr<ConsoleReporter> reporter_;
  BenchTargetUPtr target_;
  IOBuffer payload_;

  uint64_t deadline_us_{0};
  std::atomic<bool> stop_{false};
  std::atomic<uint64_t> bytes_issued_{0};
  Profiler profiler_;
};

}  // namespace store
}  // namespace bench
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_TOOLS_BENCH_STORE_RUNNER_H_
