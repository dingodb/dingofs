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

#ifndef _GNU_SOURCE
#define _GNU_SOURCE  // for fallocate()
#endif

#include "cache/tools/bench/fsop/runner.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "cache/tools/bench/common/format.h"

namespace dingofs {
namespace cache {
namespace bench {
namespace fsop {

const char* const kOpNames[kNumOps] = {
    "mkdir",  "open(w)", "fallocate", "write", "fsync",    "close(w)",
    "rename", "link",    "open(r)",   "read",  "close(r)", "unlink",
};

namespace {

constexpr uint64_t kBufAlign = 4096;
constexpr uint64_t kBuckets = 64;

inline uint64_t NowNs() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL) +
         static_cast<uint64_t>(ts.tv_nsec);
}

struct Timed {
  OpStats* stats;
  uint64_t start;
  explicit Timed(OpStats* s) : stats(s), start(NowNs()) {}
  void Stop(Op op, bool failed) const {
    stats->hist[op].Add(NowNs() - start);
    if (failed) ++stats->errors;
  }
};

void Worker(const Options& options, uint64_t block_size, uint32_t tid,
            OpStats* stats) {
  void* buf = nullptr;
  if (posix_memalign(&buf, kBufAlign, block_size) != 0 || buf == nullptr) {
    ++stats->errors;
    return;
  }
  std::memset(buf, 0, block_size);

  const std::string root = options.shared_dir
                               ? (options.dir + "/shared")
                               : (options.dir + "/t" + std::to_string(tid));
  const std::string prefix =
      options.shared_dir ? ("t" + std::to_string(tid) + "_") : "blk";
  ::mkdir(root.c_str(), 0755);  // setup, not measured

  const int wflags =
      O_CREAT | O_WRONLY | O_TRUNC | (options.direct ? O_DIRECT : 0);
  const int rflags = O_RDONLY | (options.direct ? O_DIRECT : 0);

  for (uint32_t i = 0; i < options.iters; ++i) {
    const std::string bucket = root + "/b" + std::to_string(i % kBuckets);
    const std::string base = bucket + "/" + prefix + std::to_string(i);
    const std::string tmp = base + ".tmp";
    const std::string final = base + ".blk";
    const std::string clink = base + ".cache";

    {
      Timed t(stats);
      const int rc = ::mkdir(bucket.c_str(), 0755);
      t.Stop(kMkdir, rc != 0 && errno != EEXIST);
    }

    int fd;
    {
      Timed t(stats);
      fd = ::open(tmp.c_str(), wflags, 0644);
      t.Stop(kOpenW, fd < 0);
    }
    if (fd < 0) continue;

    if (options.fallocate) {
      Timed t(stats);
      const int rc = ::fallocate(fd, 0, 0, static_cast<off_t>(block_size));
      t.Stop(kFallocate, rc != 0);
    }
    {
      Timed t(stats);
      const ssize_t n = ::pwrite(fd, buf, block_size, 0);
      t.Stop(kWrite, n != static_cast<ssize_t>(block_size));
    }
    if (options.fsync) {
      Timed t(stats);
      const int rc = ::fdatasync(fd);
      t.Stop(kFsync, rc != 0);
    }
    {
      Timed t(stats);
      const int rc = ::close(fd);
      t.Stop(kCloseW, rc != 0);
    }
    {
      Timed t(stats);
      const int rc = ::rename(tmp.c_str(), final.c_str());
      t.Stop(kRename, rc != 0);
    }
    {
      Timed t(stats);
      const int rc = ::link(final.c_str(), clink.c_str());
      t.Stop(kLink, rc != 0);
    }

    int rfd;
    {
      Timed t(stats);
      rfd = ::open(final.c_str(), rflags);
      t.Stop(kOpenR, rfd < 0);
    }
    if (rfd >= 0) {
      {
        Timed t(stats);
        const ssize_t n = ::pread(rfd, buf, block_size, 0);
        t.Stop(kRead, n != static_cast<ssize_t>(block_size));
      }
      {
        Timed t(stats);
        const int rc = ::close(rfd);
        t.Stop(kCloseR, rc != 0);
      }
    }

    {
      Timed t(stats);
      const int rc1 = ::unlink(clink.c_str());
      const int rc2 = ::unlink(final.c_str());
      t.Stop(kUnlink, rc1 != 0 || rc2 != 0);
    }
  }

  free(buf);
}

}  // namespace

void OpStats::Merge(const OpStats& other) {
  for (int i = 0; i < kNumOps; ++i) {
    hist[i].Merge(other.hist[i]);
  }
  errors += other.errors;
}

OpStats RunCell(const Options& options, uint64_t block_size, uint32_t threads) {
  std::vector<OpStats> per_thread(threads);
  std::vector<std::thread> pool;
  pool.reserve(threads);
  for (uint32_t t = 0; t < threads; ++t) {
    pool.emplace_back([&options, block_size, t, &per_thread]() {
      Worker(options, block_size, t, &per_thread[t]);
    });
  }
  for (auto& th : pool) th.join();

  OpStats merged;
  for (const auto& s : per_thread) merged.Merge(s);
  return merged;
}

void PrintCell(const Options& options, uint64_t block_size, uint32_t threads,
               const OpStats& stats) {
  uint64_t total_ns = 0;
  uint64_t write_path_ns = 0;
  uint64_t read_path_ns = 0;
  for (int i = 0; i < kNumOps; ++i) {
    const uint64_t sum = stats.hist[i].Sum();
    total_ns += sum;
    if (i >= kMkdir && i <= kLink) {
      write_path_ns += sum;
    } else if (i >= kOpenR && i <= kCloseR) {
      read_path_ns += sum;
    }
  }
  const double denom = total_ns == 0 ? 1.0 : static_cast<double>(total_ns);

  std::cout << "\n=== block=" << FormatBytes(block_size)
            << "  threads=" << threads << "  iters=" << options.iters
            << "/thread (" << static_cast<uint64_t>(options.iters) * threads
            << " cycles) ===\n";
  std::cout << "  " << std::left << std::setw(11) << "op" << std::right
            << std::setw(11) << "mean" << std::setw(11) << "p50"
            << std::setw(11) << "p99" << std::setw(8) << "share" << '\n';

  for (int i = 0; i < kNumOps; ++i) {
    const auto& h = stats.hist[i];
    if (h.Count() == 0) continue;
    std::ostringstream share;
    share << std::fixed << std::setprecision(1) << (h.Sum() * 100.0 / denom)
          << "%";
    std::cout << "  " << std::left << std::setw(11) << kOpNames[i] << std::right
              << std::setw(11) << FormatNanos(h.Mean()) << std::setw(11)
              << FormatNanos(h.Percentile(0.50)) << std::setw(11)
              << FormatNanos(h.Percentile(0.99)) << std::setw(8) << share.str()
              << '\n';
  }

  std::ostringstream wp;
  std::ostringstream rp;
  wp << std::fixed << std::setprecision(1) << (write_path_ns * 100.0 / denom)
     << "%";
  rp << std::fixed << std::setprecision(1) << (read_path_ns * 100.0 / denom)
     << "%";
  std::cout << "  " << std::string(50, '-') << '\n';
  std::cout << "  write-path (mkdir..link): " << wp.str()
            << "   read-path (open(r)..close(r)): " << rp.str() << '\n';
  if (stats.errors > 0) {
    std::cout << "  errors: " << stats.errors << '\n';
  }
}

}  // namespace fsop
}  // namespace bench
}  // namespace cache
}  // namespace dingofs
