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

#include "cache/tools/bench/fsop/command.h"

#include <algorithm>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include "cache/tools/bench/common/flags.h"
#include "cache/tools/bench/common/format.h"
#include "cache/tools/bench/fsop/options.h"
#include "cache/tools/bench/fsop/runner.h"

namespace dingofs {
namespace cache {
namespace bench {
namespace fsop {

namespace {

struct CellSummary {
  uint64_t block_size;
  uint32_t threads;
  uint64_t cycle_ns;
  double write_path_pct;
  std::string slowest_op;
};

CellSummary Summarize(uint64_t block_size, uint32_t threads,
                      const OpStats& stats) {
  uint64_t cycle_ns = 0;
  uint64_t total_ns = 0;
  uint64_t write_path_ns = 0;
  uint64_t slowest_ns = 0;
  const char* slowest = "-";
  for (int i = 0; i < kNumOps; ++i) {
    cycle_ns += stats.hist[i].Mean();
    const uint64_t sum = stats.hist[i].Sum();
    total_ns += sum;
    if (i >= kMkdir && i <= kLink) write_path_ns += sum;
    if (sum > slowest_ns) {
      slowest_ns = sum;
      slowest = kOpNames[i];
    }
  }
  const double pct = total_ns == 0 ? 0.0 : write_path_ns * 100.0 / total_ns;
  return CellSummary{block_size, threads, cycle_ns, pct, slowest};
}

void Cleanup(const Options& o) {
  if (o.keep) return;
  uint32_t max_threads = 0;
  for (uint32_t t : o.threads) max_threads = std::max(max_threads, t);
  std::error_code ec;
  std::filesystem::remove_all(o.dir + "/shared", ec);
  for (uint32_t t = 0; t < max_threads; ++t) {
    std::filesystem::remove_all(o.dir + "/t" + std::to_string(t), ec);
  }
}

}  // namespace

int Run(const std::string& program, int argc, char** argv) {
  Options o;
  FlagSet fs;
  RegisterFlags(&fs, &o);

  std::string err;
  if (!fs.Parse(argc, argv, &err)) {
    std::cerr << "error: " << err << "  (run `" << program << " --help`)\n";
    return 1;
  }
  if (fs.HelpRequested()) {
    fs.PrintHelp(program, program + " --dir=DIR [OPTIONS]",
                 "  $ cb fsop --dir=/data/t --sizes=4KiB,4MiB --threads=1,8 --iters=500\n"
                 "  $ cb fsop --dir=/data/t --sizes=4KiB --threads=1,8,32 --shared_dir\n");
    return 0;
  }
  err = Validate(&o);
  if (!err.empty()) {
    std::cerr << "error: " << err << "  (run `" << program << " --help`)\n";
    return 1;
  }

  std::filesystem::create_directories(o.dir);

  std::cout << "\nDingoFS cb fsop (syscall breakdown)\n";
  std::cout << "==================================\n";
  std::cout << "  dir        " << o.dir << '\n';
  std::cout << "  O_DIRECT   " << (o.direct ? "yes" : "no") << '\n';
  std::cout << "  fallocate  " << (o.fallocate ? "yes" : "no") << '\n';
  std::cout << "  fsync      " << (o.fsync ? "yes" : "no") << '\n';
  std::cout << "  layout     "
            << (o.shared_dir ? "shared (all threads one dir tree)"
                             : "isolated (per-thread dir tree)")
            << '\n';
  std::cout << "  iters      " << o.iters << "/thread per cell\n";

  std::vector<CellSummary> summary;
  for (uint64_t bs : o.sizes) {
    for (uint32_t t : o.threads) {
      const OpStats stats = RunCell(o, bs, t);
      PrintCell(o, bs, t, stats);
      summary.push_back(Summarize(bs, t, stats));
    }
  }

  Cleanup(o);

  std::cout << "\nSummary (mean time of one write+read cycle)\n";
  std::cout << "-------------------------------------------\n";
  std::cout << "  " << std::left << std::setw(9) << "block" << std::setw(9)
            << "threads" << std::right << std::setw(12) << "cycle"
            << std::setw(13) << "write-path" << "   slowest op\n";
  for (const auto& cell : summary) {
    std::cout << "  " << std::left << std::setw(9)
              << FormatBytes(cell.block_size) << std::setw(9) << cell.threads
              << std::right << std::setw(12) << FormatNanos(cell.cycle_ns)
              << std::setw(11) << std::fixed << std::setprecision(1)
              << cell.write_path_pct << "%"
              << "   " << cell.slowest_op << '\n';
  }

  return 0;
}

}  // namespace fsop
}  // namespace bench
}  // namespace cache
}  // namespace dingofs
