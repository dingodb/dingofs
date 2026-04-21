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

/*
 * DingoFS Write Benchmark — bypass FUSE, call libdingofs C API directly.
 *
 * This tool stress-tests the client-side write path (VFS -> ChunkWriter ->
 * SliceWriter -> BlockStore) without FUSE kernel overhead.  Useful for:
 *   - Profiling client internals with `perf record`
 *   - Measuring streaming upload throughput
 *   - Comparing MDS mode vs local mode performance
 *   - Isolating client bottlenecks with FakeBlockStore
 *
 * Usage:
 *   write_bench --bench_mds_addr=10.0.0.1:8801 --bench_fs_name=myfs \
 *               --bench_threads=8 --bench_file_size_mb=1024
 *
 *   # Local mode
 *   write_bench --bench_mds_addr="local://myfs" --bench_fs_name=myfs \
 *               --bench_threads=4 --bench_file_size_mb=512
 *
 *   # FakeBlockStore (isolate client CPU)
 *   write_bench --bench_mds_addr=10.0.0.1:8801 --bench_fs_name=myfs \
 *               --bench_threads=16 --bench_file_size_mb=2048 \
 *               --bench_fake_blockstore
 *
 *   # With config file
 *   write_bench --bench_mds_addr=10.0.0.1:8801 --bench_fs_name=myfs \
 *               --bench_threads=8 --bench_conf=/etc/dingofs/dingo.yaml
 */

#include <fcntl.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <pthread.h>
#include <time.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <thread>
#include <vector>

#include "c/libdingofs.h"

// ---------------------------------------------------------------------------
// gflags
// ---------------------------------------------------------------------------

// All flags prefixed with "bench_" to avoid gflags conflicts with flags
// defined inside libdingofs.so (which statically links the full client).
DEFINE_string(bench_mds_addr, "",
              "MDS address (e.g. 10.0.0.1:8801) or local://fs");
DEFINE_string(bench_fs_name, "", "Filesystem name");
DEFINE_string(bench_mount_point, "/bench_mount", "Logical mount point label");
DEFINE_string(bench_conf, "", "Config file path (gflags format)");
DEFINE_string(bench_dir, "/bench", "Directory inside FS for bench files");
DEFINE_int32(bench_dummy_port, 0,
             "Override vfs_dummy_server_port (0 = use default 10000)");

DEFINE_int32(bench_threads, 1, "Number of concurrent writer threads");
DEFINE_int32(bench_file_size_mb, 1024, "File size per thread in MB");
DEFINE_int32(bench_block_size_kb, 4096, "Write block size in KB (default 4MB)");

DEFINE_bool(bench_fake_blockstore, false,
            "Use FakeBlockStore to bypass BlockStore+Cache+IO entirely");
DEFINE_bool(bench_fake_access, false,
            "Use FakeAccesser to bypass IO only, keep TierBlockCache path");
DEFINE_bool(bench_cleanup, true, "Remove bench files after completion");
DEFINE_bool(bench_fsync, false, "Call fsync after each file write");
DEFINE_bool(bench_flush, true, "Call flush after each file write");
DEFINE_bool(bench_progress, true, "Show live progress during benchmark");

DEFINE_string(bench_log_dir, "/tmp", "Log directory for glog");
DEFINE_string(bench_log_level, "WARNING",
              "Log level: INFO/WARNING/ERROR/FATAL");

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static double NowSec() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec + ts.tv_nsec * 1e-9;
}

static std::string HumanSize(size_t bytes) {
  const char* units[] = {"B", "KB", "MB", "GB", "TB"};
  int idx = 0;
  double val = static_cast<double>(bytes);
  while (val >= 1024.0 && idx < 4) {
    val /= 1024.0;
    idx++;
  }
  char buf[64];
  (void)snprintf(buf, sizeof(buf), "%.1f %s", val, units[idx]);
  return buf;
}

// ---------------------------------------------------------------------------
// Per-thread result
// ---------------------------------------------------------------------------

struct ThreadResult {
  int thread_id;
  size_t bytes_written;
  double elapsed_sec;
  double throughput_mbs;
  int error_code;
};

// ---------------------------------------------------------------------------
// Progress tracker
// ---------------------------------------------------------------------------

class ProgressTracker {
 public:
  ProgressTracker(size_t total_bytes, int num_threads)
      : total_bytes_(total_bytes),
        num_threads_(num_threads),
        written_(0),
        running_(true) {
    if (FLAGS_bench_progress) {
      thread_ = std::thread([this]() { Run(); });
    }
  }

  ~ProgressTracker() { Stop(); }

  void Add(size_t bytes) {
    written_.fetch_add(bytes, std::memory_order_relaxed);
  }

  void Stop() {
    running_ = false;
    if (thread_.joinable()) {
      thread_.join();
    }
  }

 private:
  void Run() {
    auto start = std::chrono::steady_clock::now();
    while (running_) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      if (!running_) break;

      auto now = std::chrono::steady_clock::now();
      double elapsed = std::chrono::duration<double>(now - start).count();
      size_t w = written_.load(std::memory_order_relaxed);
      double pct = 100.0 * w / total_bytes_;
      double mbs = (w / (1024.0 * 1024.0)) / elapsed;

      std::cout << "\r  [" << num_threads_ << "T] " << std::fixed
                << std::setprecision(1) << pct << "%  " << HumanSize(w) << " / "
                << HumanSize(total_bytes_) << "  " << std::setprecision(1)
                << mbs << " MB/s  " << std::flush;
    }
  }

  size_t total_bytes_;
  int num_threads_;
  std::atomic<size_t> written_;
  std::atomic<bool> running_;
  std::thread thread_;
};

// ---------------------------------------------------------------------------
// Writer thread
// ---------------------------------------------------------------------------

static void WriterThread(uintptr_t handle, int thread_id, size_t file_size,
                         size_t block_size, ProgressTracker* progress,
                         ThreadResult* result) {
  result->thread_id = thread_id;
  result->bytes_written = 0;
  result->elapsed_sec = 0;
  result->throughput_mbs = 0;
  result->error_code = 0;

  // Per-thread subdirectory (pre-created by main thread to avoid MDS
  // TxnWriteConflict on concurrent mkdir under the same parent).
  char dir[512];
  (void)snprintf(dir, sizeof(dir), "%s/%d", FLAGS_bench_dir.c_str(), thread_id);

  char path[512];
  (void)snprintf(path, sizeof(path), "%s/data", dir);

  // Open file (O_WRONLY | O_CREAT | O_TRUNC)
  int fd = dingofs_open(handle, path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) {
    std::cerr << "thread " << thread_id << ": open failed: " << fd << "\n";
    result->error_code = fd;
    return;
  }

  // Allocate write buffer (fill with recognizable pattern)
  std::vector<char> buf(block_size);
  std::memset(buf.data(), 'A' + (thread_id % 26), block_size);

  // Write loop using pwrite
  double t0 = NowSec();
  size_t written = 0;
  while (written < file_size) {
    size_t to_write = std::min(block_size, file_size - written);
    ssize_t n = dingofs_pwrite(handle, fd, buf.data(), to_write, written);
    if (n < 0) {
      std::cerr << "thread " << thread_id << ": write failed at offset "
                << written << ": " << n << "\n";
      result->error_code = static_cast<int>(n);
      break;
    }
    written += n;
    if (progress) {
      progress->Add(n);
    }
  }

  // Flush / fsync
  if (FLAGS_bench_flush) {
    dingofs_flush(handle, fd);
  }
  if (FLAGS_bench_fsync) {
    dingofs_fsync(handle, fd, 0);
  }

  dingofs_close(handle, fd);
  double t1 = NowSec();

  result->bytes_written = written;
  result->elapsed_sec = t1 - t0;
  result->throughput_mbs = (written / (1024.0 * 1024.0)) / result->elapsed_sec;

  // Cleanup file and per-thread directory
  if (FLAGS_bench_cleanup) {
    dingofs_unlink(handle, path);
    dingofs_rmdir(handle, dir);
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

static void PrintUsage(const char* prog) {
  std::cout << R"(
DingoFS Write Benchmark — bypass FUSE, call libdingofs C API directly.

Usage:
  )" << prog << R"( --bench_mds_addr=ADDR --bench_fs_name=NAME [options]

Required:
  --bench_mds_addr       MDS address (e.g. 10.0.0.1:8801) or local://fs
  --bench_fs_name        Filesystem name

Options:
  --bench_threads          Number of writer threads (default: 1)
  --bench_file_size_mb     File size per thread in MB (default: 1024)
  --bench_block_size_kb    Write block size in KB (default: 4096 = 4MB)
  --bench_dir              Directory inside FS for bench files (default: /bench)
  --bench_mount_point      Logical mount point label (default: /bench_mount)
  --bench_conf             Config file path (gflags format)
  --bench_fake_blockstore  Use FakeBlockStore to bypass real I/O
  --bench_cleanup          Remove bench files after completion (default: true)
  --bench_flush            Call flush after file write (default: true)
  --bench_fsync            Call fsync after file write (default: false)
  --bench_progress         Show live progress (default: true)
  --bench_log_dir          Log directory (default: /tmp)
  --bench_log_level        Log level: INFO/WARNING/ERROR/FATAL (default: WARNING)

Examples:
  # MDS mode, 8 threads, 1GB per thread
  )" << prog << R"( --bench_mds_addr=10.0.0.1:8801 --bench_fs_name=myfs \
      --bench_threads=8 --bench_file_size_mb=1024

  # Local mode
  )" << prog << R"( --bench_mds_addr="local://myfs" --bench_fs_name=myfs \
      --bench_threads=4 --bench_file_size_mb=512

  # FakeBlockStore (measure pure client CPU overhead)
  )" << prog << R"( --bench_mds_addr=10.0.0.1:8801 --bench_fs_name=myfs \
      --bench_threads=16 --bench_file_size_mb=2048 --bench_fake_blockstore

  # With perf profiling
  perf record -g -F 99 -- )"
            << prog << R"( --bench_mds_addr=10.0.0.1:8801 \
      --bench_fs_name=myfs --bench_threads=8 --bench_file_size_mb=2048 \
      --bench_fake_blockstore
)";
}

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage("DingoFS Write Benchmark (bypass FUSE)");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_bench_mds_addr.empty() || FLAGS_bench_fs_name.empty()) {
    PrintUsage(argv[0]);
    return 1;
  }

  // Compute sizes
  size_t file_size =
      static_cast<size_t>(FLAGS_bench_file_size_mb) * 1024 * 1024;
  size_t block_size = static_cast<size_t>(FLAGS_bench_block_size_kb) * 1024;
  int nthreads = FLAGS_bench_threads;

  // Print banner
  std::cout << "DingoFS Write Benchmark (bypass FUSE)"
            << "\n";
  std::cout << "  mds_addr:       " << FLAGS_bench_mds_addr << "\n";
  std::cout << "  fs_name:        " << FLAGS_bench_fs_name << "\n";
  std::cout << "  threads:        " << nthreads << "\n";
  std::cout << "  file_size:      " << FLAGS_bench_file_size_mb << " MB"
            << "\n";
  std::cout << "  block_size:     " << FLAGS_bench_block_size_kb << " KB"
            << "\n";
  std::cout << "  total_write:    "
            << HumanSize(static_cast<size_t>(nthreads) * file_size) << "\n";
  std::cout << "  fake_blockstore:"
            << (FLAGS_bench_fake_blockstore ? " yes" : " no") << "\n";
  std::cout << "  fake_access:    " << (FLAGS_bench_fake_access ? "yes" : "no")
            << "\n";
  std::cout << "  flush:          " << (FLAGS_bench_flush ? "yes" : "no")
            << "\n";
  std::cout << "  fsync:          " << (FLAGS_bench_fsync ? "yes" : "no")
            << "\n";
  if (!FLAGS_bench_conf.empty()) {
    std::cout << "  conf:           " << FLAGS_bench_conf << "\n";
  }
  std::cout << "\n";

  // Create and configure instance
  uintptr_t h = dingofs_new();

  // Set log options
  dingofs_conf_set(h, "log.dir", FLAGS_bench_log_dir.c_str());
  dingofs_conf_set(h, "log.level", FLAGS_bench_log_level.c_str());

  // Load config file if specified
  if (!FLAGS_bench_conf.empty()) {
    int rc = dingofs_conf_load(h, FLAGS_bench_conf.c_str());
    if (rc != 0) {
      std::cerr << "Failed to load config " << FLAGS_bench_conf << ": " << rc
                << "\n";
      dingofs_delete(h);
      return 1;
    }
  }

  // Dummy server port (avoid conflict with FUSE client on same machine)
  if (FLAGS_bench_dummy_port > 0) {
    dingofs_conf_set(h, "vfs_dummy_server_port",
                     std::to_string(FLAGS_bench_dummy_port).c_str());
  }

  // FakeBlockStore (skip BlockStore + Cache + IO entirely)
  if (FLAGS_bench_fake_blockstore) {
    dingofs_conf_set(h, "vfs_use_fake_block_store", "true");
    std::cout << "FakeBlockStore: enabled"
              << "\n";
  }

  // FakeAccesser (skip IO only, keep TierBlockCache path)
  if (FLAGS_bench_fake_access) {
    dingofs_conf_set(h, "use_fake_block_access", "true");
    std::cout << "FakeAccesser: enabled"
              << "\n";
  }

  // Mount
  std::cout << "Mounting " << FLAGS_bench_mds_addr << "/" << FLAGS_bench_fs_name
            << " ..."
            << "\n";
  int rc = dingofs_mount(h, FLAGS_bench_mds_addr.c_str(),
                         FLAGS_bench_fs_name.c_str(),
                         FLAGS_bench_mount_point.c_str());
  if (rc != 0) {
    std::cerr << "Mount failed: " << rc << "\n";
    dingofs_delete(h);
    return 1;
  }

  // Pre-create bench directory and per-thread subdirectories serially
  // to avoid MDS TxnWriteConflict on the parent inode.
  dingofs_mkdir(h, FLAGS_bench_dir.c_str(), 0755);
  for (int i = 0; i < nthreads; i++) {
    char dir[512];
    (void)snprintf(dir, sizeof(dir), "%s/%d", FLAGS_bench_dir.c_str(), i);
    dingofs_mkdir(h, dir, 0755);
  }

  // Prepare progress tracker
  size_t total_bytes = static_cast<size_t>(nthreads) * file_size;
  ProgressTracker progress(total_bytes, nthreads);

  // Launch writer threads
  std::vector<std::thread> threads;
  std::vector<ThreadResult> results(nthreads);

  double t_start = NowSec();

  threads.reserve(nthreads);
  for (int i = 0; i < nthreads; i++) {
    threads.emplace_back(WriterThread, h, i, file_size, block_size, &progress,
                         &results[i]);
  }

  for (auto& t : threads) {
    t.join();
  }

  double t_end = NowSec();
  progress.Stop();

  if (FLAGS_bench_progress) {
    std::cout << "\r" << std::string(80, ' ') << "\r";
  }

  // Check for errors
  bool has_error = false;
  for (const auto& r : results) {
    if (r.error_code != 0) {
      has_error = true;
    }
  }

  // Report
  double wall_sec = t_end - t_start;
  double total_data_mb = 0;
  for (const auto& r : results) {
    total_data_mb += r.bytes_written / (1024.0 * 1024.0);
  }

  std::cout << "\n";
  std::cout << "=== Results ==="
            << "\n";
  std::cout << std::left << std::setw(10) << "Thread" << std::right
            << std::setw(12) << "Written" << std::setw(12) << "Elapsed"
            << std::setw(14) << "Throughput" << std::setw(8) << "Status"
            << "\n";
  std::cout << std::string(56, '-') << "\n";

  for (const auto& r : results) {
    std::cout << std::left << std::setw(10) << r.thread_id << std::right
              << std::setw(10)
              << static_cast<int>(r.bytes_written / (1024.0 * 1024.0)) << " MB"
              << std::setw(9) << std::fixed << std::setprecision(2)
              << r.elapsed_sec << " s" << std::setw(10) << std::setprecision(1)
              << r.throughput_mbs << " MB/s" << std::setw(8)
              << (r.error_code == 0 ? "  OK" : "  FAIL") << "\n";
  }

  std::cout << std::string(56, '-') << "\n";
  std::cout << "Aggregate: " << std::fixed << std::setprecision(1)
            << total_data_mb << " MB in " << std::setprecision(2) << wall_sec
            << " s = " << std::setprecision(1) << total_data_mb / wall_sec
            << " MB/s"
            << "\n";

  // Cleanup bench directory if empty
  if (FLAGS_bench_cleanup) {
    dingofs_rmdir(h, FLAGS_bench_dir.c_str());
  }

  // Umount
  dingofs_umount(h);
  dingofs_delete(h);

  return has_error ? 1 : 0;
}
