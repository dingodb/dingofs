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
 * DingoFS Metadata Benchmark (mdtest-like) — bypass FUSE and the VFS layer,
 * call the MDSClient C++ API directly.
 *
 * This tool stress-tests the client-side metadata path (client -> MDS) with
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
#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <pthread.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "client/vfs/common/client_id.h"
#include "client/vfs/metasystem/mds/helper.h"
#include "client/vfs/metasystem/mds/mds_client.h"
#include "client/vfs/metasystem/mds/rpc.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/meta.h"
#include "common/status.h"
#include "common/trace/context.h"
#include "common/trace/trace_manager.h"
#include "common/types.h"
#include "dingofs/error.pb.h"
#include "dingofs/mds.pb.h"
#include "mds/filesystem/fs_info.h"
#include "utils/concurrent/concurrent.h"
#include "utils/uuid.h"

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
DEFINE_int32(bench_thread_group, 1, "Number of concurrent worker thread group");
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
DEFINE_string(bench_log_level, "ERROR", "Log level: INFO/WARNING/ERROR/FATAL");

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

using dingofs::Attr;
using dingofs::Context;
using dingofs::ContextSPtr;
using dingofs::DirEntry;
using dingofs::Ino;
using dingofs::ReadDirHandler;
using dingofs::Status;
using dingofs::TraceManager;
using dingofs::client::vfs::ClientId;
using dingofs::client::vfs::meta::Helper;
using dingofs::client::vfs::meta::MDSClient;
using dingofs::client::vfs::meta::RPC;

// ---------------------------------------------------------------------------
// MdsDirectClient — a thin adapter over MDSClient that exposes the small
// metadata API this benchmark needs (Lookup/MkDir/Create/Release/Unlink/
// RmDir/ReadDir). It bypasses the whole VFS layer (no inode/dentry cache, no
// handle manager, no data plane) and talks straight to the MDS via MDSClient.
// ---------------------------------------------------------------------------
class MdsDirectClient {
 public:
  MdsDirectClient() = default;
  ~MdsDirectClient() = default;

  Status Start(const std::string& mds_addrs, const std::string& fs_name,
               const std::string& mount_point) {
    fs_name_ = fs_name;

    RPC rpc(mds_addrs, "bench");
    auto status = rpc.Init();
    if (!status.ok()) {
      return Status::Internal("rpc init fail");
    }

    dingofs::mds::FsInfoEntry fs_info_entry;
    Status s = MDSClient::GetFsInfo(rpc, fs_name, fs_info_entry);
    if (!s.ok()) return s;

    fs_info_ = std::make_unique<dingofs::mds::FsInfo>(fs_info_entry);

    trace_manager_ = std::make_unique<TraceManager>();
    trace_manager_->Init();

    const std::string hostname = dingofs::Helper::GetHostName();
    client_id_ =
        ClientId(dingofs::utils::GenerateUUID(), hostname, 0, mount_point);

    mds_client_ = std::make_unique<MDSClient>(client_id_, *fs_info_,
                                              std::move(rpc), *trace_manager_);
    if (!mds_client_->Init()) {
      return Status::Internal("mds client init fail");
    }

    dingofs::pb::mds::MountPoint mp;
    mp.set_client_id(client_id_.ID());
    mp.set_hostname(client_id_.Hostname());
    mp.set_ip(client_id_.IP());
    mp.set_port(client_id_.Port());
    mp.set_path(client_id_.Mountpoint());
    mp.set_cto(false);
    s = mds_client_->MountFs(fs_name_, mp);
    if (!s.ok() && s.Errno() != dingofs::pb::error::EEXISTED) {
      return s;
    }

    return Status::OK();
  }

  void Stop() {
    if (mds_client_ == nullptr) return;
    mds_client_->UmountFs(fs_name_, client_id_.ID());
    mds_client_->Stop();
    if (trace_manager_ != nullptr) trace_manager_->Stop();
  }

  Status Lookup(Ino parent, const std::string& name, Attr* attr) {
    ContextSPtr ctx = NewCtx();
    dingofs::mds::AttrEntry attr_entry;
    Status s = mds_client_->Lookup(ctx, parent, name, attr_entry);
    if (!s.ok()) return s;
    *attr = Helper::ToAttr(attr_entry);
    return Status::OK();
  }

  Status MkDir(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
               uint32_t mode, Attr* attr) {
    ContextSPtr ctx = NewCtx();
    dingofs::mds::AttrEntry attr_entry, parent_attr_entry;
    Status s = mds_client_->MkDir(ctx, parent, name, uid, gid, mode, 0,
                                  attr_entry, parent_attr_entry);
    if (!s.ok()) return s;
    *attr = Helper::ToAttr(attr_entry);
    return Status::OK();
  }

  Status Create(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
                uint32_t mode, int flags, Attr* attr) {
    ContextSPtr ctx = NewCtx();
    dingofs::mds::AttrEntry attr_entry, parent_attr_entry;
    Status s = mds_client_->MkNod(ctx, parent, name, uid, gid, mode, 0,
                                  attr_entry, parent_attr_entry);
    if (!s.ok()) return s;
    *attr = Helper::ToAttr(attr_entry);

    return Status::OK();
  }

  Status Release(Ino ino, uint64_t fh) {
    std::string session_id;
    {
      dingofs::utils::WriteLockGuard lk(session_lock_);
      auto it = fh_sessions_.find(fh);
      if (it == fh_sessions_.end()) return Status::OK();
      session_id = it->second;
      fh_sessions_.erase(it);
    }
    ContextSPtr ctx = NewCtx();
    return mds_client_->Release(ctx, ino, session_id);
  }

  Status Unlink(Ino parent, const std::string& name) {
    ContextSPtr ctx = NewCtx();
    dingofs::mds::AttrEntry attr_entry, parent_attr_entry;
    return mds_client_->UnLink(ctx, parent, name, attr_entry,
                               parent_attr_entry);
  }

  Status RmDir(Ino parent, const std::string& name) {
    ContextSPtr ctx = NewCtx();
    Ino ino = 0;
    dingofs::mds::AttrEntry parent_attr_entry;
    return mds_client_->RmDir(ctx, parent, name, ino, parent_attr_entry);
  }

  // The VFS-level OpenDir/ReleaseDir only allocated a local handle; talking to
  // the MDS directly needs none, so these are no-ops kept for call-site
  // compatibility.
  Status OpenDir(Ino /*ino*/, uint64_t* fh, bool /*need_cache*/) {
    *fh = 0;
    return Status::OK();
  }

  Status ReleaseDir(Ino /*ino*/, uint64_t /*fh*/) { return Status::OK(); }

  // Reads the whole directory by paging on last_name until a short batch is
  // returned, invoking `handler` for every entry (offset is synthesized).
  Status ReadDir(Ino ino, uint64_t fh, uint64_t /*offset*/, bool with_attr,
                 const ReadDirHandler& handler) {
    constexpr uint32_t kBatch = 1024;
    std::string last_name;
    uint64_t off = 0;
    for (;;) {
      ContextSPtr ctx = NewCtx();
      std::vector<DirEntry> entries;
      Status s = mds_client_->ReadDir(ctx, ino, fh, last_name, kBatch,
                                      with_attr, entries);
      if (!s.ok()) return s;
      for (const auto& entry : entries) {
        if (!handler(entry, off++)) return Status::OK();
      }
      if (entries.size() < kBatch) break;
      last_name = entries.back().name;
    }
    return Status::OK();
  }

 private:
  ContextSPtr NewCtx() { return std::make_shared<Context>(std::string()); }

  static std::vector<std::string> SplitMdsAddrs(const std::string& addrs) {
    std::vector<std::string> out;
    const char sep = (addrs.find(',') != std::string::npos)   ? ','
                     : (addrs.find(';') != std::string::npos) ? ';'
                                                              : '\0';
    if (sep == '\0') {
      out.push_back(addrs);
      return out;
    }
    size_t start = 0;
    while (start <= addrs.size()) {
      size_t pos = addrs.find(sep, start);
      if (pos == std::string::npos) {
        out.push_back(addrs.substr(start));
        break;
      }
      out.push_back(addrs.substr(start, pos - start));
      start = pos + 1;
    }
    return out;
  }

  std::string fs_name_;
  ClientId client_id_;
  std::unique_ptr<TraceManager> trace_manager_;
  std::unique_ptr<dingofs::mds::FsInfo> fs_info_;
  std::unique_ptr<MDSClient> mds_client_;

  std::atomic<uint64_t> next_fh_{1};
  dingofs::utils::RWLock session_lock_;
  std::unordered_map<uint64_t, std::string> fh_sessions_;
};

static double NowSec() {
  struct timespec ts{};
  if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
    return 0.0;
  }
  return ts.tv_sec + (ts.tv_nsec * 1e-9);
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

static int StatusToErrno(const Status& s) {
  if (s.ok()) return 0;
  return -s.ToSysErrNo();
}

// /a/b/c -> c
static std::string BaseName(const std::string& path) {
  size_t slash = path.rfind('/');
  if (slash == std::string::npos) return path;
  return path.substr(slash + 1);
}

// /a/b/c -> /a/b
static std::string DirName(const std::string& path) {
  size_t slash = path.rfind('/');
  if (slash == std::string::npos) return ".";
  if (slash == 0) return "/";
  return path.substr(0, slash);
}

// Split an absolute path into components. Leading/trailing '/' are ignored and
// empty components (from consecutive slashes) are skipped.
static std::vector<std::string> SplitPath(const std::string& path) {
  std::vector<std::string> parts;
  size_t i = 0;
  while (i < path.size() && path[i] == '/') i++;
  while (i < path.size()) {
    size_t j = path.find('/', i);
    if (j == std::string::npos) {
      parts.push_back(path.substr(i));
      break;
    }
    parts.push_back(path.substr(i, j - i));
    i = j + 1;
    while (i < path.size() && path[i] == '/') i++;
  }
  return parts;
}

// Resolve an absolute path to its inode. Returns 0 on success, -errno on
// failure.
static int ResolvePath(MdsDirectClient* client, const std::string& path,
                       Ino* ino) {
  std::vector<std::string> parts = SplitPath(path);
  Ino parent = dingofs::kRootIno;
  for (const auto& name : parts) {
    Attr attr;
    Status s = client->Lookup(parent, name, &attr);
    if (!s.ok()) {
      LOG(ERROR) << fmt::format("resolve path fail, path({}) error({}).", path,
                                s.ToString());
      return StatusToErrno(s);
    }
    parent = attr.ino;
  }
  *ino = parent;
  return 0;
}

// Resolve the parent directory of `path` and extract the final component name.
static int ResolveParent(MdsDirectClient* client, const std::string& path,
                         Ino* parent_ino, std::string* name) {
  if (path.empty() || path[0] != '/') return -EINVAL;

  size_t end = path.size();
  while (end > 1 && path[end - 1] == '/') end--;
  size_t slash = path.rfind('/', end - 1);
  if (slash == std::string::npos) return -EINVAL;

  std::string parent_path = (slash == 0) ? "/" : path.substr(0, slash);
  *name = path.substr(slash + 1, end - slash - 1);

  if (name->empty()) return -EINVAL;
  return ResolvePath(client, parent_path, parent_ino);
}

// ---------------------------------------------------------------------------
// Index-based tree spec — replaces TreePaths/GenerateTree.
//
// Paths are computed on-demand from (level, index) using the identity:
//   component at level l of directory (level=d, global index=i):
//     (i / width^(d-l)) % width
// This eliminates O(total_files * path_len) pre-allocation; the only
// per-spec storage is O(depth) for the precomputed power table.
// ---------------------------------------------------------------------------

struct TreeSpec {
  std::string root;
  int depth;
  int width;
  int files_per_leaf;
  std::vector<uint64_t> pow_width;  // pow_width[k] = width^k

  mutable std::unique_ptr<dingofs::utils::RWLock> lock_;
  std::unordered_map<std::string, Ino> dir_ino_map;

  TreeSpec(std::string r, int d, int w, int f)
      : root(std::move(r)),
        depth(d),
        width(w),
        files_per_leaf(f),
        lock_(std::make_unique<dingofs::utils::RWLock>()) {
    pow_width.resize(d + 1);
    pow_width[0] = 1;
    for (int k = 1; k <= d; k++) pow_width[k] = pow_width[k - 1] * w;
  }

  // Number of directories at BFS level `d` (0 = first level below root).
  uint64_t DirsAtLevel(int d) const { return pow_width[d + 1]; }
  uint64_t TotalLeaves() const { return pow_width[depth]; }
  uint64_t TotalFiles() const {
    return TotalLeaves() * static_cast<uint64_t>(files_per_leaf);
  }

  // Path of the directory at BFS level `d`, global index `idx`.
  // Index ordering matches the BFS order produced by the old GenerateTree.
  std::string DirPath(int d, uint64_t idx) const {
    std::string p = root;
    for (int l = 0; l <= d; l++) {
      uint64_t comp = (idx / pow_width[d - l]) % width;
      p += "/d" + std::to_string(l) + "_" + std::to_string(comp);
    }
    return p;
  }

  // Path of a file identified by its global index across all files in this
  // tree.
  std::string FilePath(uint64_t global_idx) const {
    uint64_t leaf_idx = global_idx / static_cast<uint64_t>(files_per_leaf);
    uint64_t file_idx = global_idx % files_per_leaf;
    return DirPath(depth - 1, leaf_idx) + "/f" + std::to_string(file_idx);
  }

  void SaveDirIno(const std::string& path, Ino ino) {
    dingofs::utils::WriteLockGuard lk(*lock_);

    dir_ino_map[path] = ino;
    LOG(INFO) << "Saved ino " << ino << " for dir " << path;
  }

  Ino ResolveParent(MdsDirectClient* client, const std::string& path) {
    const std::string dir_path = DirName(path);

    {
      dingofs::utils::ReadLockGuard lk(*lock_);

      auto it = dir_ino_map.find(dir_path);
      if (it != dir_ino_map.end()) return it->second;
    }

    Ino ino;
    CHECK(ResolvePath(client, dir_path, &ino) == 0)
        << fmt::format("resolve parent dir fail, path({}).", dir_path);
    SaveDirIno(dir_path, ino);

    return ino;
  }
};

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

static int CreateDir(MdsDirectClient* client, const std::string& path) {
  Ino parent;
  std::string name;
  int rc = ResolveParent(client, path, &parent, &name);
  if (rc != 0) return rc;
  Attr attr;
  Status s = client->MkDir(parent, name, getuid(), getgid(), 0755, &attr);
  if (!s.ok()) {
    LOG(ERROR) << fmt::format("create dir fail, parent({}) name({}) error({}).",
                              parent, name, s.ToString());
  }

  return StatusToErrno(s);
}

static int CreateDir(MdsDirectClient* client, Ino parent,
                     const std::string& name, Ino& ino) {
  CHECK(parent != 0) << "Invalid parent inode 0 for " << name;

  Attr attr;
  Status s = client->MkDir(parent, name, getuid(), getgid(), 0755, &attr);
  if (!s.ok()) {
    LOG(ERROR) << fmt::format("create dir fail, parent({}) name({}) error({}).",
                              parent, name, s.ToString());
  }

  ino = attr.ino;

  return StatusToErrno(s);
}

static int UnlinkPath(MdsDirectClient* client, const std::string& path) {
  Ino parent;
  std::string name;
  int rc = ResolveParent(client, path, &parent, &name);
  if (rc != 0) return rc;
  Status s = client->Unlink(parent, name);
  if (!s.ok()) {
    LOG(ERROR) << fmt::format("unlink fail, parent({}) name({}) error({}).",
                              parent, name, s.ToString());
  }
  return StatusToErrno(s);
}

static int RemoveDir(MdsDirectClient* client, const std::string& path) {
  Ino parent;
  std::string name;
  int rc = ResolveParent(client, path, &parent, &name);
  if (rc != 0) return rc;
  Status s = client->RmDir(parent, name);
  if (!s.ok()) {
    LOG(ERROR) << fmt::format("rmdir fail, parent({}) name({}) error({}).",
                              parent, name, s.ToString());
  }

  return StatusToErrno(s);
}

static int CreateEmptyFile(MdsDirectClient* client, Ino parent,
                           const std::string& name) {
  CHECK(parent != 0) << "Invalid parent inode 0 for " << name;

  Attr attr;
  Status s = client->Create(parent, name, getuid(), getgid(), 33188,
                            O_CREAT | O_WRONLY | O_EXCL, &attr);
  if (!s.ok()) {
    LOG(ERROR) << fmt::format(
        "create file fail, parent({}) name({}) error({}).", parent, name,
        s.ToString());
    return StatusToErrno(s);
  }

  return StatusToErrno(s);
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
static void UniqueWorker(MdsDirectClient* client, TreeSpec* spec,
                         pthread_barrier_t* barrier,
                         ProgressTracker* dir_progress,
                         ProgressTracker* file_progress, ThreadResult* result) {
  // ---- Directory phase ----
  pthread_barrier_wait(barrier);
  for (int d = 0; d < spec->depth && result->error_code == 0; d++) {
    uint64_t count = spec->DirsAtLevel(d);
    for (uint64_t i = 0; i < count; i++) {
      std::string path = spec->DirPath(d, i);
      std::string name = BaseName(path);
      Ino parent = spec->ResolveParent(client, path);
      CHECK(parent != 0) << "Parent directory not found for " << path;

      Ino ino;
      int rc = CreateDir(client, parent, name, ino);
      if (rc != 0) {
        LOG(ERROR) << fmt::format(
            "create dir fail, thread({}) path({}) error({}).",
            result->thread_id, path, strerror(-rc));

        result->error_code = rc;
        result->error_path = std::move(path);
        break;
      }
      spec->SaveDirIno(path, ino);

      result->dir_ops++;
      dir_progress->Add(1);
    }
  }
  pthread_barrier_wait(barrier);

  // ---- File phase ----
  pthread_barrier_wait(barrier);
  if (result->error_code == 0) {
    uint64_t total = spec->TotalFiles();
    for (uint64_t g = 0; g < total; g++) {
      std::string path = spec->FilePath(g);
      Ino parent = spec->ResolveParent(client, path);
      std::string name = BaseName(path);
      CHECK(parent != 0) << fmt::format("not found parent directory, path({}).",
                                        path);

      int rc = CreateEmptyFile(client, parent, name);
      if (rc != 0) {
        LOG(ERROR) << fmt::format(
            "create file fail, thread({}) path({}) error({}).",
            result->thread_id, name, strerror(-rc));
        result->error_code = rc;
        result->error_path = std::move(name);
        break;
      }
      result->file_ops++;
      file_progress->Add(1);
    }
  }
  pthread_barrier_wait(barrier);
}

// shared mode: atomic-cursor dispatch over index ranges; paths computed
// on-demand from indices.  A failed thread stops taking tasks but keeps
// hitting every barrier to avoid deadlock.
struct SharedWork {
  struct FileCursor {
    std::atomic<uint64_t> index{0};
    std::atomic<uint64_t> limit{0};

    FileCursor() = default;
    FileCursor(FileCursor&& other) noexcept
        : index(other.index.load()), limit(other.limit.load()) {}
  };
  std::unique_ptr<std::atomic<uint64_t>[]> dir_cursors;  // one per level

  uint32_t files_per_leaf{0};
  uint32_t step_size{0};
  std::vector<FileCursor> file_cursors;

  explicit SharedWork(int depth, uint32_t thread_groups,
                      uint32_t files_per_leaf)
      : dir_cursors(std::make_unique<std::atomic<uint64_t>[]>(depth)),
        files_per_leaf(files_per_leaf),
        step_size(files_per_leaf * (thread_groups - 1)) {
    CHECK(thread_groups > 0) << "thread_groups must be > 0";
    CHECK(files_per_leaf > 0) << "files_per_leaf must be > 0";

    for (int i = 0; i < depth; ++i) {
      dir_cursors[i].store(0);
    }

    file_cursors.resize(thread_groups);
    for (uint32_t i = 0; i < thread_groups; ++i) {
      auto& cursor = file_cursors[i];
      cursor.index.store(i * files_per_leaf);
      cursor.limit.store(cursor.index.load() + files_per_leaf);
    }

    Print();
  }

  uint64_t GetFileIndex(int group_num) {
    auto& cursor = file_cursors[group_num];

    uint64_t file_index{0};
    do {
      uint64_t limit = cursor.limit.load(std::memory_order_relaxed);
      file_index = cursor.index.fetch_add(1, std::memory_order_relaxed);

      if (file_index < limit) {
        if (file_index + files_per_leaf >= limit)
          break;
        else
          std::this_thread::sleep_for(std::chrono::milliseconds(1));

      } else if (file_index == limit) {
        // Move to the next batch for this group
        uint64_t new_index = limit + step_size;
        cursor.index.store(new_index);
        cursor.limit.store(new_index + files_per_leaf);

        LOG(INFO) << fmt::format(
            "[sharedwork] group({}) reached limit({}) moving to next batch "
            "[{},{})",
            group_num, limit, new_index, new_index + files_per_leaf);

      } else {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }

    } while (true);

    return file_index;
  }

  void Print() {
    LOG(INFO) << fmt::format("[sharedwork] files_per_leaf({}) step_size({}).",
                             files_per_leaf, step_size);
    for (size_t i = 0; i < file_cursors.size(); ++i) {
      const auto& cursor = file_cursors[i];
      LOG(INFO) << fmt::format("[sharedwork] group({}) index[{}, {}).", i,
                               cursor.index.load(), cursor.limit.load());
    }
  }
};

static void SharedWorker(MdsDirectClient* client, TreeSpec* spec,
                         SharedWork* work, pthread_barrier_t* barrier,
                         ProgressTracker* dir_progress,
                         ProgressTracker* file_progress, ThreadResult* result) {
  // ---- Directory phase: one level at a time ----
  pthread_barrier_wait(barrier);
  for (int d = 0; d < spec->depth; d++) {
    if (result->error_code == 0) {
      uint64_t count = spec->DirsAtLevel(d);
      for (;;) {
        uint64_t idx =
            work->dir_cursors[d].fetch_add(1, std::memory_order_relaxed);
        if (idx >= count) break;
        std::string path = spec->DirPath(d, idx);
        Ino parent = spec->ResolveParent(client, path);
        CHECK(parent != 0) << fmt::format(
            "not found parent directory, path({}).", path);

        std::string name = BaseName(path);
        Ino ino = 0;
        int rc = CreateDir(client, parent, name, ino);
        if (rc != 0) {
          LOG(ERROR) << fmt::format(
              "create dir fail, thread({}) path({}) error({}).",
              result->thread_id, path, strerror(-rc));
          result->error_code = rc;
          result->error_path = std::move(path);
          break;
        }
        spec->SaveDirIno(path, ino);

        result->dir_ops++;
        dir_progress->Add(1);
      }
    }
    // All threads must finish this level before the next starts so that
    // parent directories exist when child directories are created.
    pthread_barrier_wait(barrier);
  }
  pthread_barrier_wait(barrier);

  // ---- File phase ----
  pthread_barrier_wait(barrier);
  if (result->error_code == 0) {
    uint32_t group_num = result->thread_id % work->file_cursors.size();
    uint64_t total = spec->TotalFiles();
    for (;;) {
      uint64_t file_index = work->GetFileIndex(group_num);
      if (file_index >= total) break;

      std::string path = spec->FilePath(file_index);
      Ino parent = spec->ResolveParent(client, path);
      std::string name = BaseName(path);
      CHECK(parent != 0) << fmt::format("not found parent directory, path({}).",
                                        path);

      int rc = CreateEmptyFile(client, parent, name);
      if (rc != 0) {
        LOG(ERROR) << fmt::format(
            "create file fail, thread({}) path({}) error({}).",
            result->thread_id, name, strerror(-rc));
        result->error_code = rc;
        result->error_path = std::move(name);
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

static int RemoveTreeRecursive(MdsDirectClient* client,
                               const std::string& path) {
  Ino dir_ino;
  int rc = ResolvePath(client, path, &dir_ino);
  if (rc != 0) return rc;

  uint64_t fh = 0;
  bool need_cache = false;
  Status s = client->OpenDir(dir_ino, &fh, need_cache);
  if (!s.ok()) return StatusToErrno(s);

  std::vector<DirEntry> entries;
  dingofs::ReadDirHandler handler = [&](const DirEntry& entry,
                                        uint64_t offset) -> bool {
    (void)offset;
    if (entry.name == "." || entry.name == "..") return true;
    entries.push_back(entry);
    return true;
  };

  s = client->ReadDir(dir_ino, fh, 0, true, handler);
  if (!s.ok()) {
    client->ReleaseDir(dir_ino, fh);
    return StatusToErrno(s);
  }
  client->ReleaseDir(dir_ino, fh);

  int first_err = 0;
  for (const auto& entry : entries) {
    std::string child = path + "/" + entry.name;
    int crc = (entry.attr.type == dingofs::kDirectory)
                  ? RemoveTreeRecursive(client, child)
                  : UnlinkPath(client, child);
    if (crc != 0 && first_err == 0) first_err = crc;
  }

  if (first_err != 0) return first_err;
  return RemoveDir(client, path);
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

  // ---- Configure logging before MdsDirectClient starts ----
  FLAGS_log_dir = FLAGS_bench_log_dir;
  dingofs::FLAGS_log_level = FLAGS_bench_log_level;

  if (!FLAGS_bench_conf.empty()) {
    if (!gflags::ReadFromFlagsFile(FLAGS_bench_conf, argv[0], true)) {
      std::cerr << "Failed to load config " << FLAGS_bench_conf << "\n";
      return 1;
    }
  }

  // init global log
  dingofs::Logger::Init("dingo-mdtest-bench");

  // ---- Mount ----
  auto client = std::make_unique<MdsDirectClient>();

  std::cout << "Mounting " << FLAGS_bench_mds_addr << "/" << FLAGS_bench_fs_name
            << " ...\n";
  Status s = client->Start(FLAGS_bench_mds_addr, FLAGS_bench_fs_name,
                           FLAGS_bench_mount_point);
  if (!s.ok()) {
    std::cerr << "Mount failed: " << s.ToString() << "\n";
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
    Ino ino;
    if (ResolvePath(client.get(), root, &ino) == 0) {
      has_residue = true;
      break;
    }
  }
  if (has_residue) {
    if (!FLAGS_bench_force) {
      std::cerr << "Error: residual tree from a previous run exists under "
                << FLAGS_bench_dir
                << ". Re-run with --bench_force to remove it first.\n";
      client->Stop();
      return 1;
    }
    std::cout << "Removing residual tree (--bench_force) ...\n";
    for (const auto& root : roots) {
      Ino ino;
      if (ResolvePath(client.get(), root, &ino) != 0) continue;
      int rrc = RemoveTreeRecursive(client.get(), root);
      if (rrc != 0) {
        std::cerr << "Failed to remove residual " << root << ": " << rrc
                  << "\n";
        client->Stop();
        return 1;
      }
    }
  }

  // ---- Pre-create untimed roots serially (avoids MDS TxnWriteConflict
  // on the parent inode, same as write_bench) ----
  CreateDir(client.get(), FLAGS_bench_dir);  // may already exist
  for (const auto& root : roots) {
    int rc = CreateDir(client.get(), root);
    if (rc != 0) {
      std::cerr << "Failed to create root " << root << ": " << rc << "\n";
      client->Stop();
      return 1;
    }
  }

  // ---- Build tree specs (O(depth) memory each, no path pre-allocation) ----
  std::vector<TreeSpec> specs;
  specs.reserve(ntrees);
  for (int i = 0; i < ntrees; i++) {
    specs.emplace_back(roots[i], FLAGS_bench_depth, FLAGS_bench_width,
                       FLAGS_bench_files);
  }

  // ---- Run ----
  pthread_barrier_t barrier;
  pthread_barrier_init(&barrier, nullptr, nthreads + 1);  // +1 = main (timer)

  std::vector<ThreadResult> results(nthreads);
  for (int i = 0; i < nthreads; i++) results[i].thread_id = i;

  ProgressTracker dir_progress(total_dirs, nthreads, "dirs");
  ProgressTracker file_progress(total_files, nthreads, "files");

  std::unique_ptr<SharedWork> shared_work;
  if (shared_mode)
    shared_work = std::make_unique<SharedWork>(
        FLAGS_bench_depth, FLAGS_bench_thread_group, FLAGS_bench_files);

  std::vector<std::thread> workers;
  workers.reserve(nthreads);
  for (int i = 0; i < nthreads; i++) {
    if (shared_mode) {
      workers.emplace_back(SharedWorker, client.get(), specs.data(),
                           shared_work.get(), &barrier, &dir_progress,
                           &file_progress, &results[i]);
    } else {
      workers.emplace_back(UniqueWorker, client.get(), &specs[i], &barrier,
                           &dir_progress, &file_progress, &results[i]);
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
      int crc = RemoveTreeRecursive(client.get(), root);
      if (crc != 0) {
        std::cerr << "Warning: cleanup of " << root << " failed: " << crc
                  << "\n";
      }
    }
    RemoveDir(client.get(), FLAGS_bench_dir);  // best effort
  } else {
    std::cout << "\nCleanup skipped; tree left under " << FLAGS_bench_dir
              << "\n";
  }

  client->Stop();

  return nerrors > 0 ? 1 : 0;
}
