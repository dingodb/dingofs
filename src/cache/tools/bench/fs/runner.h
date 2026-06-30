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

#ifndef DINGOFS_SRC_CACHE_TOOLS_BENCH_FS_RUNNER_H_
#define DINGOFS_SRC_CACHE_TOOLS_BENCH_FS_RUNNER_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include "cache/local/disk_cache_layout.h"
#include "cache/local/local_filesystem.h"
#include "cache/tools/bench/common/profiler.h"
#include "cache/tools/bench/common/reporter.h"
#include "cache/tools/bench/common/stats.h"
#include "cache/tools/bench/fs/options.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace bench {
namespace fs {

// Each op is a whole-block WriteFile/ReadFile via LocalFileSystem; `jobs`
// pthread workers drive it (InflightTracker caps the AIO at iodepth).
class Runner {
 public:
  explicit Runner(Options options);
  ~Runner();

  Status Run();

 private:
  Status Init();
  Status PrepBlocks();
  void RunWorker(uint32_t id);
  std::string BlockPath(uint64_t key) const;
  void Shutdown();

  Options options_;
  std::unique_ptr<Stats> stats_;
  std::unique_ptr<ConsoleReporter> reporter_;
  DiskCacheLayoutSPtr layout_;
  LocalFileSystemUPtr fs_;
  IOBuffer payload_;

  uint64_t deadline_us_{0};
  std::atomic<bool> stop_{false};
  std::atomic<uint64_t> bytes_issued_{0};
  Profiler profiler_;
};

}  // namespace fs
}  // namespace bench
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_TOOLS_BENCH_FS_RUNNER_H_
