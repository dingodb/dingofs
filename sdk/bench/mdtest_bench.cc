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
 * DingoFS Metadata Benchmark (mdtest-like) — bypass FUSE, call libdingofs
 * C API directly.
 *
 * This tool stress-tests the client-side metadata path (VFS -> MDS) with
 * concurrent directory and file creation, similar to mdtest with -I -L
 * semantics (files only in leaf directories).
 *
 * Tree layout (per tree): each directory has `width` subdirectories,
 * `depth` levels deep; each leaf directory holds `files` empty files.
 *
 * Concurrency modes:
 *   unique (default)  Each thread works in its own subtree
 *                     <bench_dir>/thread_<i>/ — measures scalability.
 *   shared            One global tree under <bench_dir>/shared; threads
 *                     take creation tasks from a shared queue, so sibling
 *                     mkdir/create hit the same parent inode concurrently
 *                     and trigger MDS transaction conflicts (absorbed by
 *                     internal client retries; the cost shows up as lower
 *                     ops/s compared to unique mode with same parameters).
 *
 * Only the creation phases are measured (directory phase, then file
 * phase). Cleanup is optional and never timed.
 *
 * Note: results reflect the client's view (dentry caching applies); they
 * measure warm-client creation throughput, not cold-cache lookups.
 *
 * Usage:
 *   # unique mode, defaults: depth=3 width=4 files=10 threads=1
 *   mdtest_bench --bench_mds_addr=10.0.0.1:8801 --bench_fs_name=myfs
 *
 *   # shared mode contention test, compare against unique mode
 *   mdtest_bench --bench_mds_addr=10.0.0.1:8801 --bench_fs_name=myfs \
 *                --bench_mode=shared --bench_threads=16 \
 *                --bench_depth=2 --bench_width=8 --bench_files=100
 *
 *   # force-remove residue from a previous interrupted run
 *   mdtest_bench [...] --bench_force
 */

#include <fcntl.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <pthread.h>
#include <time.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <deque>
#include <iomanip>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "c/libdingofs.h"

// ---------------------------------------------------------------------------
// gflags
// ---------------------------------------------------------------------------

DEFINE_string(bench_mds_addr, "",
              "MDS address (e.g. 10.0.0.1:8801) or local://fs");
DEFINE_string(bench_fs_name, "", "Filesystem name");
DEFINE_string(bench_mount_point, "/mdtest_bench_mount",
              "Logical mount point label");
DEFINE_string(bench_conf, "", "Config file path (gflags format)");
DEFINE_string(bench_dir, "/mdtest_bench", "Directory inside FS for bench tree");

DEFINE_int32(bench_threads, 1, "Number of concurrent worker threads");
DEFINE_int32(bench_depth, 3, "Directory tree depth (levels, >= 1)");
DEFINE_int32(bench_width, 4, "Branching factor per directory (>= 1)");
DEFINE_int32(bench_files, 10, "Empty files per leaf directory (>= 0)");
DEFINE_string(bench_mode, "unique",
              "Concurrency mode: unique (per-thread subtree) | "
              "shared (global tree, shared task queue)");

DEFINE_bool(bench_cleanup, false, "Remove bench tree after completion");
DEFINE_bool(bench_force, true,
            "Remove residual tree from a previous run before starting");
DEFINE_bool(bench_progress, true, "Show live progress during benchmark");

DEFINE_string(bench_log_dir, "/tmp/bench_log", "Log directory for glog");
DEFINE_string(bench_log_level, "INFO", "Log level: INFO/WARNING/ERROR/FATAL");

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static double NowSec() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec + ts.tv_nsec * 1e-9;
}

static double SafeOpsPerSec(uint64_t ops, double elapsed_sec) {
  if (ops == 0 || elapsed_sec <= 1e-9) return 0.0;
  return static_cast<double>(ops) / elapsed_sec;
}

// Directories in one tree: width^1 + ... + width^depth.
// Leaves in one tree: width^depth.
// Returns false on overflow.
static bool TreeCounts(int depth, int width, uint64_t* dirs, uint64_t* leaves) {
  uint64_t total = 0;
  uint64_t level = 1;
  for (int d = 1; d <= depth; d++) {
    if (level > UINT64_MAX / static_cast<uint64_t>(width)) return false;
    level *= static_cast<uint64_t>(width);
    if (total > UINT64_MAX - level) return false;
    total += level;
  }
  *dirs = total;
  *leaves = level;
  return true;
}

// ---------------------------------------------------------------------------
// Tree path generation
// ---------------------------------------------------------------------------

// Directory paths of one tree grouped by level (level index 0 = first level
// below `root`), generated in BFS order so parents always precede children.
struct TreePaths {
  std::vector<std::vector<std::string>>
      levels;                      // levels[d] = dirs at depth d+1
  std::vector<std::string> files;  // leaf files
};

static TreePaths GenerateTree(const std::string& root, int depth, int width,
                              int files_per_leaf) {
  TreePaths tree;
  tree.levels.resize(depth);

  std::vector<std::string> parents = {root};
  for (int d = 0; d < depth; d++) {
    auto& level = tree.levels[d];
    level.reserve(parents.size() * width);
    for (const auto& parent : parents) {
      for (int w = 0; w < width; w++) {
        level.push_back(parent + "/d" + std::to_string(d) + "_" +
                        std::to_string(w));
      }
    }
    parents = level;
  }

  // `parents` now holds the leaf directories.
  tree.files.reserve(parents.size() * files_per_leaf);
  for (const auto& leaf : parents) {
    for (int j = 0; j < files_per_leaf; j++) {
      tree.files.push_back(leaf + "/f" + std::to_string(j));
    }
  }
  return tree;
}

// ---------------------------------------------------------------------------
// Progress tracker (ops-based)
// ---------------------------------------------------------------------------

class ProgressTracker {
 public:
  ProgressTracker(uint64_t total_ops, int num_threads, const char* phase)
      : total_ops_(total_ops),
        num_threads_(num_threads),
        phase_(phase),
        progressed_(0),
        running_(false) {}

  ~ProgressTracker() { Stop(); }

  // Spawn the printer thread and capture the rate baseline. Call at the
  // start of the phase this tracker reports on.
  void Start() {
    if (!FLAGS_bench_progress || total_ops_ == 0 || running_) return;
    running_ = true;
    thread_ = std::thread([this]() { Run(); });
  }

  void Add(uint64_t ops) {
    progressed_.fetch_add(ops, std::memory_order_relaxed);
  }

  void Stop() {
    running_ = false;
    if (thread_.joinable()) {
      thread_.join();
      std::cout << "\n";
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
      uint64_t p = progressed_.load(std::memory_order_relaxed);
      double pct = 100.0 * p / total_ops_;
      double ops_sec = SafeOpsPerSec(p, elapsed);

      std::cout << "\r  [" << phase_ << " " << num_threads_ << "T] "
                << std::fixed << std::setprecision(1) << pct << "%  " << p
                << " / " << total_ops_ << " ops  " << std::setprecision(0)
                << ops_sec << " ops/s  " << std::flush;
    }
  }

  uint64_t total_ops_;
  int num_threads_;
  const char* phase_;
  std::atomic<uint64_t> progressed_;
  std::atomic<bool> running_;
  std::thread thread_;
};

// ---------------------------------------------------------------------------
// Worker state
// ---------------------------------------------------------------------------

struct ThreadResult {
  int thread_id = 0;
  uint64_t dir_ops = 0;
  uint64_t file_ops = 0;
  int error_code = 0;      // first error (-errno), 0 = OK
  std::string error_path;  // path of first error
};

static int CreateDir(uintptr_t h, const std::string& path) {
  return dingofs_mkdir(h, path.c_str(), 0755);
}

static int CreateEmptyFile(uintptr_t h, const std::string& path) {
  int fd = dingofs_open(h, path.c_str(), O_CREAT | O_WRONLY | O_EXCL, 0644);
  if (fd < 0) return fd;
  return dingofs_close(h, fd);
}

// ---------------------------------------------------------------------------
// Phase runners
//
// Both modes execute two timed phases (directories, then files). Threads
// synchronize on a barrier before each phase; phase wall time is measured
// by thread 0 across barrier entry/exit of all workers.
// ---------------------------------------------------------------------------

// unique mode: thread i creates its whole subtree (BFS order).
// On error the thread records it and stops; other threads keep going
// (the thread still participates in all barriers to avoid deadlock).
static void UniqueWorker(uintptr_t h, const TreePaths* tree,
                         pthread_barrier_t* barrier,
                         ProgressTracker* dir_progress,
                         ProgressTracker* file_progress, ThreadResult* result) {
  // ---- Directory phase ----
  pthread_barrier_wait(barrier);
  for (const auto& level : tree->levels) {
    for (const auto& path : level) {
      int rc = CreateDir(h, path);
      if (rc != 0) {
        result->error_code = rc;
        result->error_path = path;
        break;
      }
      result->dir_ops++;
      dir_progress->Add(1);
    }
    if (result->error_code != 0) break;
  }
  pthread_barrier_wait(barrier);

  // ---- File phase ----
  pthread_barrier_wait(barrier);
  if (result->error_code == 0) {
    for (const auto& path : tree->files) {
      int rc = CreateEmptyFile(h, path);
      if (rc != 0) {
        result->error_code = rc;
        result->error_path = path;
        break;
      }
      result->file_ops++;
      file_progress->Add(1);
    }
  }
  pthread_barrier_wait(barrier);
}

// shared mode: all threads drain a per-batch task list via an atomic
// cursor. Batches are dispatched level by level (parents before children),
// with a barrier between batches; leaf files form the final batch.
// A failed thread stops taking tasks but keeps hitting every barrier.
struct SharedBatch {
  const std::vector<std::string>* tasks = nullptr;
  std::atomic<uint64_t> cursor{0};
};

static void SharedWorker(uintptr_t h, std::deque<SharedBatch>* dir_batches,
                         SharedBatch* file_batch, pthread_barrier_t* barrier,
                         ProgressTracker* dir_progress,
                         ProgressTracker* file_progress, ThreadResult* result) {
  // ---- Directory phase: one batch per level ----
  pthread_barrier_wait(barrier);
  for (auto& batch : *dir_batches) {
    if (result->error_code == 0) {
      const auto& tasks = *batch.tasks;
      for (;;) {
        uint64_t idx = batch.cursor.fetch_add(1, std::memory_order_relaxed);
        if (idx >= tasks.size()) break;
        int rc = CreateDir(h, tasks[idx]);
        if (rc != 0) {
          result->error_code = rc;
          result->error_path = tasks[idx];
          break;
        }
        result->dir_ops++;
        dir_progress->Add(1);
      }
    }
    // All threads must finish this level before the next one starts,
    // so parent directories exist when children are created.
    pthread_barrier_wait(barrier);
  }
  pthread_barrier_wait(barrier);

  // ---- File phase ----
  pthread_barrier_wait(barrier);
  if (result->error_code == 0) {
    const auto& tasks = *file_batch->tasks;
    for (;;) {
      uint64_t idx = file_batch->cursor.fetch_add(1, std::memory_order_relaxed);
      if (idx >= tasks.size()) break;
      int rc = CreateEmptyFile(h, tasks[idx]);
      if (rc != 0) {
        result->error_code = rc;
        result->error_path = tasks[idx];
        break;
      }
      result->file_ops++;
      file_progress->Add(1);
    }
  }
  pthread_barrier_wait(barrier);
}

// ---------------------------------------------------------------------------
// Recursive removal (post-order: unlink files, rmdir bottom-up).
// Walks the real tree via readdir so it can remove residue of any shape.
// Returns 0 on success, first -errno on failure.
// ---------------------------------------------------------------------------

static int RemoveTreeRecursive(uintptr_t h, const std::string& path) {
  uint64_t dh = 0;
  int rc = dingofs_opendir(h, path.c_str(), &dh);
  if (rc != 0) return rc;

  int first_err = 0;
  dingofs_dirent_t entries[128];
  for (;;) {
    int n =
        dingofs_readdir(h, dh, entries, sizeof(entries) / sizeof(entries[0]));
    if (n < 0) {
      first_err = n;
      break;
    }
    if (n == 0) break;
    for (int i = 0; i < n; i++) {
      const char* name = entries[i].d_name;
      if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) continue;
      std::string child = path + "/" + name;
      int crc = (entries[i].d_type == DT_DIR)
                    ? RemoveTreeRecursive(h, child)
                    : dingofs_unlink(h, child.c_str());
      if (crc != 0 && first_err == 0) first_err = crc;
    }
  }
  dingofs_closedir(h, dh);

  if (first_err != 0) return first_err;
  return dingofs_rmdir(h, path.c_str());
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

static void PrintPhaseLine(const char* phase, uint64_t ops, double elapsed) {
  std::cout << "  " << std::left << std::setw(12) << phase << std::right
            << std::setw(12) << ops << " ops" << std::setw(10) << std::fixed
            << std::setprecision(2) << elapsed << " s" << std::setw(12)
            << std::setprecision(0) << SafeOpsPerSec(ops, elapsed) << " ops/s"
            << "\n";
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main(int argc, char** argv) {
  gflags::SetUsageMessage(
      "DingoFS metadata creation benchmark (mdtest-like)\n"
      "Required: --bench_mds_addr --bench_fs_name");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // ---- Validate parameters ----
  if (FLAGS_bench_mds_addr.empty() || FLAGS_bench_fs_name.empty()) {
    std::cerr << "Error: --bench_mds_addr and --bench_fs_name are required\n";
    return 1;
  }
  if (FLAGS_bench_depth < 1) {
    std::cerr << "Error: --bench_depth must be >= 1\n";
    return 1;
  }
  if (FLAGS_bench_width < 1) {
    std::cerr << "Error: --bench_width must be >= 1\n";
    return 1;
  }
  if (FLAGS_bench_files < 0) {
    std::cerr << "Error: --bench_files must be >= 0\n";
    return 1;
  }
  if (FLAGS_bench_threads < 1) {
    std::cerr << "Error: --bench_threads must be >= 1\n";
    return 1;
  }
  const bool shared_mode = (FLAGS_bench_mode == "shared");
  if (!shared_mode && FLAGS_bench_mode != "unique") {
    std::cerr << "Error: --bench_mode must be 'unique' or 'shared'\n";
    return 1;
  }

  const int nthreads = FLAGS_bench_threads;
  const int ntrees = shared_mode ? 1 : nthreads;

  uint64_t dirs_per_tree = 0;
  uint64_t leaves_per_tree = 0;
  if (!TreeCounts(FLAGS_bench_depth, FLAGS_bench_width, &dirs_per_tree,
                  &leaves_per_tree)) {
    std::cerr << "Error: depth/width combination overflows\n";
    return 1;
  }

  // Roots (thread_<i> or shared) are created outside the timed phases.
  const uint64_t total_dirs = dirs_per_tree * ntrees;
  const uint64_t total_files =
      leaves_per_tree * ntrees * static_cast<uint64_t>(FLAGS_bench_files);

  constexpr uint64_t kSanityLimit = 10ULL * 1000 * 1000;
  if (total_dirs + total_files > kSanityLimit) {
    std::cout << "WARNING: this run will create " << total_dirs
              << " directories and " << total_files
              << " files; make sure the MDS and client can handle it.\n";
  }

  std::cout << "DingoFS Metadata Benchmark (mdtest-like)\n";
  std::cout << "  mds_addr:    " << FLAGS_bench_mds_addr << "\n";
  std::cout << "  fs_name:     " << FLAGS_bench_fs_name << "\n";
  std::cout << "  bench_dir:   " << FLAGS_bench_dir << "\n";
  std::cout << "  mode:        " << FLAGS_bench_mode << "\n";
  std::cout << "  threads:     " << nthreads << "\n";
  std::cout << "  depth:       " << FLAGS_bench_depth << "\n";
  std::cout << "  width:       " << FLAGS_bench_width << "\n";
  std::cout << "  files/leaf:  " << FLAGS_bench_files << "\n";
  std::cout << "  total dirs:  " << total_dirs << " (+" << ntrees
            << " untimed root(s))\n";
  std::cout << "  total files: " << total_files << "\n";
  std::cout << "\n";

  // ---- Mount ----
  uintptr_t h = dingofs_new();
  dingofs_conf_set(h, "log.dir", FLAGS_bench_log_dir.c_str());
  dingofs_conf_set(h, "log.level", FLAGS_bench_log_level.c_str());

  if (!FLAGS_bench_conf.empty()) {
    int rc = dingofs_conf_load(h, FLAGS_bench_conf.c_str());
    if (rc != 0) {
      std::cerr << "Failed to load config " << FLAGS_bench_conf << ": " << rc
                << "\n";
      dingofs_delete(h);
      return 1;
    }
  }

  std::cout << "Mounting " << FLAGS_bench_mds_addr << "/" << FLAGS_bench_fs_name
            << " ...\n";
  int rc = dingofs_mount(h, FLAGS_bench_mds_addr.c_str(),
                         FLAGS_bench_fs_name.c_str(),
                         FLAGS_bench_mount_point.c_str());
  if (rc != 0) {
    std::cerr << "Mount failed: " << rc << "\n";
    dingofs_delete(h);
    return 1;
  }

  // ---- Residue pre-check (MDS create is a blind write: a rerun over
  // residue would silently overwrite dentries, so detect it up front) ----
  std::vector<std::string> roots;
  if (shared_mode) {
    roots.push_back(FLAGS_bench_dir + "/shared");
  } else {
    for (int i = 0; i < nthreads; i++) {
      roots.push_back(FLAGS_bench_dir + "/thread_" + std::to_string(i));
    }
  }

  bool has_residue = false;
  for (const auto& root : roots) {
    struct stat st;
    if (dingofs_stat(h, root.c_str(), &st) == 0) {
      has_residue = true;
      break;
    }
  }
  if (has_residue) {
    if (!FLAGS_bench_force) {
      std::cerr << "Error: residual tree from a previous run exists under "
                << FLAGS_bench_dir
                << ". Re-run with --bench_force to remove it first.\n";
      dingofs_umount(h);
      dingofs_delete(h);
      return 1;
    }
    std::cout << "Removing residual tree (--bench_force) ...\n";
    for (const auto& root : roots) {
      struct stat st;
      if (dingofs_stat(h, root.c_str(), &st) != 0) continue;
      int rrc = RemoveTreeRecursive(h, root);
      if (rrc != 0) {
        std::cerr << "Failed to remove residual " << root << ": " << rrc
                  << "\n";
        dingofs_umount(h);
        dingofs_delete(h);
        return 1;
      }
    }
  }

  // ---- Pre-create untimed roots serially (avoids MDS TxnWriteConflict
  // on the parent inode, same as write_bench) ----
  dingofs_mkdir(h, FLAGS_bench_dir.c_str(), 0755);  // may already exist
  for (const auto& root : roots) {
    rc = dingofs_mkdir(h, root.c_str(), 0755);
    if (rc != 0) {
      std::cerr << "Failed to create root " << root << ": " << rc << "\n";
      dingofs_umount(h);
      dingofs_delete(h);
      return 1;
    }
  }

  // ---- Generate paths ----
  std::vector<TreePaths> trees;
  trees.reserve(ntrees);
  for (int i = 0; i < ntrees; i++) {
    trees.push_back(GenerateTree(roots[i], FLAGS_bench_depth, FLAGS_bench_width,
                                 FLAGS_bench_files));
  }

  // ---- Run ----
  pthread_barrier_t barrier;
  pthread_barrier_init(&barrier, nullptr, nthreads + 1);  // +1 = main (timer)

  std::vector<ThreadResult> results(nthreads);
  for (int i = 0; i < nthreads; i++) results[i].thread_id = i;

  ProgressTracker dir_progress(total_dirs, nthreads, "dirs");
  ProgressTracker file_progress(total_files, nthreads, "files");

  std::deque<SharedBatch> dir_batches;
  SharedBatch file_batch;
  if (shared_mode) {
    for (int d = 0; d < FLAGS_bench_depth; d++) {
      dir_batches.emplace_back();
      dir_batches.back().tasks = &trees[0].levels[d];
    }
    file_batch.tasks = &trees[0].files;
  }

  std::vector<std::thread> workers;
  workers.reserve(nthreads);
  for (int i = 0; i < nthreads; i++) {
    if (shared_mode) {
      workers.emplace_back(SharedWorker, h, &dir_batches, &file_batch, &barrier,
                           &dir_progress, &file_progress, &results[i]);
    } else {
      workers.emplace_back(UniqueWorker, h, &trees[i], &barrier, &dir_progress,
                           &file_progress, &results[i]);
    }
  }

  // Main thread participates in barriers purely to time each phase.
  // Phase structure (matched by both workers):
  //   dirs:  enter-barrier, [unique: work | shared: per-level barriers],
  //          exit-barrier
  //   files: enter-barrier, work, exit-barrier
  std::cout << "Phase 1: directory creation ...\n";
  pthread_barrier_wait(&barrier);  // dirs start
  double dir_start = NowSec();
  dir_progress.Start();
  if (shared_mode) {
    for (int d = 0; d < FLAGS_bench_depth; d++) {
      pthread_barrier_wait(&barrier);  // level d done
    }
  }
  pthread_barrier_wait(&barrier);  // dirs end
  double dir_elapsed = NowSec() - dir_start;
  dir_progress.Stop();

  std::cout << "Phase 2: file creation ...\n";
  pthread_barrier_wait(&barrier);  // files start
  double file_start = NowSec();
  file_progress.Start();
  pthread_barrier_wait(&barrier);  // files end
  double file_elapsed = NowSec() - file_start;
  file_progress.Stop();

  for (auto& w : workers) w.join();
  pthread_barrier_destroy(&barrier);

  // ---- Report ----
  uint64_t dir_ops = 0;
  uint64_t file_ops = 0;
  int nerrors = 0;
  for (const auto& r : results) {
    dir_ops += r.dir_ops;
    file_ops += r.file_ops;
    if (r.error_code != 0) {
      nerrors++;
      std::cerr << "Thread " << r.thread_id << " failed: " << r.error_code
                << " (" << strerror(-r.error_code) << ") at " << r.error_path
                << "\n";
    }
  }

  std::cout << "\n=== Results (" << FLAGS_bench_mode << " mode, " << nthreads
            << " threads) ===\n";
  PrintPhaseLine("dir create", dir_ops, dir_elapsed);
  PrintPhaseLine("file create", file_ops, file_elapsed);
  PrintPhaseLine("total", dir_ops + file_ops, dir_elapsed + file_elapsed);
  if (nerrors > 0) {
    std::cout << "  errors:      " << nerrors << " thread(s) failed\n";
  }

  // ---- Cleanup (untimed) ----
  if (FLAGS_bench_cleanup) {
    std::cout << "\nCleaning up ...\n";
    for (const auto& root : roots) {
      int crc = RemoveTreeRecursive(h, root);
      if (crc != 0) {
        std::cerr << "Warning: cleanup of " << root << " failed: " << crc
                  << "\n";
      }
    }
    dingofs_rmdir(h, FLAGS_bench_dir.c_str());  // best effort
  } else {
    std::cout << "\nCleanup skipped; tree left under " << FLAGS_bench_dir
              << "\n";
  }

  dingofs_umount(h);
  dingofs_delete(h);

  return nerrors > 0 ? 1 : 0;
}
