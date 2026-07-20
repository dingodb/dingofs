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
 * dingo-replay: best-effort semantic replay of a legacy `fuse_access`
 * spdlog access-log against an *empty* target DingoFS filesystem, via
 * VFSWrapper (bypassing FUSE), following the src/tools/bench/mdtest_bench.cc
 * setup pattern.
 *
 * See CONTEXT.md for terminology (Access Record, Best-effort Semantic
 * Replay, Replay Namespace, Approximate Execution, Status Divergence) and
 * src/tools/replay/replay_parser.h for the log line grammar.
 *
 * Pipeline:
 *   1. Parse every input file into ParsedRecord's (malformed/unsupported
 *      lines are reported and skipped).
 *   2. Sort by reconstructed start time (end_time - duration), stable by
 *      original sequence.
 *   3. Pre-scan (single-threaded, in sorted order): resolve each record's
 *      source-ino/source-fh references to the *record index* of their
 *      known earlier producer (lookup/mknod/symlink/link/create/mkdir for
 *      ino; open/create/opendir for fh). Records with no producer are
 *      skipped. This index-based resolution (rather than a live mutable
 *      id->id map) avoids any race from fh/ino reuse: a dependent always
 *      waits on the exact producer record instance, not a value that could
 *      be overwritten by a later generation.
 *   4. Replay: an open-loop dispatcher paces submission of records to a
 *      fixed-size thread pool (size = max inflight) by reconstructed start
 *      time / speed. Each task waits (via std::shared_future) on its
 *      producer records' completion, resolves target ino/fh, issues the
 *      VFSWrapper call, and (if it is itself a producer) publishes its
 *      resulting target ino/fh for dependents.
 */

#include <fcntl.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <fstream>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "client/vfs/vfs_wrapper.h"
#include "common/logging.h"
#include "common/meta.h"
#include "common/status.h"
#include "common/types.h"
#include "fmt/format.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "tools/replay/replay_parser.h"

// Mirrors FUSE_SET_ATTR_* from <fuse3/fuse_lowlevel.h>. Replayed directly
// (not via FUSE), so pulling in the full libfuse headers/version macro just
// for these stable protocol bit values isn't worth the dependency.
namespace {
constexpr int kFuseSetAttrMode = 1 << 0;
constexpr int kFuseSetAttrUid = 1 << 1;
constexpr int kFuseSetAttrGid = 1 << 2;
constexpr int kFuseSetAttrSize = 1 << 3;
constexpr int kFuseSetAttrAtime = 1 << 4;
constexpr int kFuseSetAttrMtime = 1 << 5;
constexpr int kFuseSetAttrAtimeNow = 1 << 7;
constexpr int kFuseSetAttrMtimeNow = 1 << 8;
}  // namespace

// ---------------------------------------------------------------------------
// gflags
// ---------------------------------------------------------------------------

DEFINE_string(replay_mds_addr, "", "MDS address (e.g. 10.0.0.1:8801)");
DEFINE_string(replay_fs_name, "", "Target filesystem name (must be empty)");
DEFINE_string(replay_mount_point, "/dingo_replay_mount",
              "Logical mount point label");
DEFINE_string(replay_conf, "", "Config file path (gflags format)");
DEFINE_string(replay_log_dir, "/tmp/dingo_replay_log",
              "Log directory for glog");
DEFINE_string(replay_log_level, "ERROR", "Log level: INFO/WARNING/ERROR/FATAL");

DEFINE_double(replay_speed, 1.0,
              "Speed multiplier applied to the open-loop schedule (>0); "
              "1.0 replays at the reconstructed original pace, 2.0 replays "
              "twice as fast, 0.5 twice as slow");
DEFINE_uint32(replay_max_inflight, 0,
              "Max concurrent in-flight operations; 0 (default) uses the "
              "observed peak overlap of reconstructed source intervals");
DEFINE_string(replay_report, "dingo_replay_report.txt",
              "Path to the detailed anomaly report (one line per malformed, "
              "skipped, approximate, or divergent record)");
DEFINE_bool(replay_self_check, false,
            "Run the embedded parser self-check and exit; requires no "
            "mounted filesystem");

namespace dingofs {
namespace tools {
namespace replay {
namespace {

using dingofs::Attr;
using dingofs::DirEntry;
using dingofs::FsStat;
using dingofs::Ino;
using dingofs::Status;
using dingofs::client::Context;
using dingofs::client::DataBuffer;
using dingofs::client::DingofsConfig;
using dingofs::client::VFSWrapper;

// ---------------------------------------------------------------------------
// Engine record: a ParsedRecord plus dependency/runtime bookkeeping.
// ---------------------------------------------------------------------------

// Sentinels for EngineRecord::ino_dep/fh_dep (indices into the records
// vector otherwise).
constexpr int64_t kNoProducer = -1;
constexpr int64_t kRootSentinel = -2;

struct EngineRecord {
  ParsedRecord p;
  std::string file;
  size_t line = 0;

  bool skip = false;
  std::string skip_reason;
  bool approximate = false;
  std::string approximate_reason;

  int num_ino_dep = 0;
  int64_t ino_dep[2] = {kNoProducer, kNoProducer};
  int num_fh_dep = 0;
  int64_t fh_dep[2] = {kNoProducer, kNoProducer};

  // Runtime outcome. Written only by this record's own worker, before it
  // fulfills `done_promise` (if allocated); read by dependents only after
  // waiting on `done_future`, which establishes the required
  // synchronizes-with relationship.
  bool replay_ok = false;
  std::string replay_status_type;
  bool produced_ino = false;
  Ino target_ino = 0;
  bool produced_fh = false;
  uint64_t target_fh = 0;
  double schedule_target_sec = 0;
  double schedule_lag_sec = 0;
  double replay_latency_sec = 0;

  // Only allocated for records that are (a) not skipped and (b) capable of
  // producing a mapping (has_result_ino or has_result_fh in the source).
  std::unique_ptr<std::promise<void>> done_promise;
  std::shared_future<void> done_future;
};

// What (source) ino(s)/fh(s) a record consumes, used to resolve producers.
struct DepSpec {
  int num_ino = 0;
  uint64_t ino[2] = {0, 0};
  int num_fh = 0;
  uint64_t fh[2] = {0, 0};
};

DepSpec ComputeDepSpec(const ParsedRecord& p) {
  DepSpec d;
  switch (p.op) {
    case OpKind::kRename:
    case OpKind::kLink:
      d.num_ino = 2;
      d.ino[0] = p.ino1;
      d.ino[1] = p.ino2;
      break;
    case OpKind::kCopyFileRange:
      d.num_ino = 2;
      d.ino[0] = p.ino1;
      d.ino[1] = p.ino2;
      d.num_fh = 2;
      d.fh[0] = p.src_fh;
      d.fh[1] = p.dst_fh;
      break;
    case OpKind::kRead:
    case OpKind::kWrite:
    case OpKind::kFlush:
    case OpKind::kRelease:
    case OpKind::kFsync:
    case OpKind::kReadDir:
    case OpKind::kReleaseDir:
      d.num_ino = 1;
      d.ino[0] = p.ino1;
      d.num_fh = 1;
      d.fh[0] = p.fh;
      break;
    default:
      // lookup/getattr/setattr/fallocate/readlink/mknod/unlink/symlink/
      // open/create/getxattr/removexattr/listxattr/mkdir/opendir/rmdir/
      // statfs all reference a single ino and nothing else.
      d.num_ino = 1;
      d.ino[0] = p.ino1;
      break;
  }
  return d;
}

// ---------------------------------------------------------------------------
// Anomaly report: file/line/category/reason for every malformed, skipped,
// approximate, or divergent record. Written from both the single-threaded
// parse/pre-scan phases and the multi-threaded replay phase.
// ---------------------------------------------------------------------------

class AnomalyReport {
 public:
  explicit AnomalyReport(const std::string& path) : out_(path) {
    out_ << "file\tline\tcategory\treason\n";
  }

  void Write(const std::string& file, size_t line, const char* category,
             const std::string& reason) {
    std::lock_guard<std::mutex> lk(mu_);
    out_ << file << '\t' << line << '\t' << category << '\t' << reason << '\n';
  }

 private:
  std::mutex mu_;
  std::ofstream out_;
};

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

constexpr size_t kNumOpKinds = static_cast<size_t>(OpKind::kUnsupported) + 1;

struct OpCounters {
  std::atomic<int64_t> exact{0};
  std::atomic<int64_t> approximate{0};
  std::atomic<int64_t> skipped{0};
  std::atomic<int64_t> diverged{0};
};

struct Stats {
  std::array<OpCounters, kNumOpKinds> per_op;

  std::mutex lat_mu;
  std::vector<double> source_lat_sec;
  std::vector<double> replay_lat_sec;
  std::vector<double> schedule_lag_sec;

  void CountSkip(OpKind op) {
    per_op[static_cast<size_t>(op)].skipped.fetch_add(1);
  }

  void CountOutcome(OpKind op, bool approximate, bool diverged,
                    double source_lat, double replay_lat) {
    auto& c = per_op[static_cast<size_t>(op)];
    if (approximate) {
      c.approximate.fetch_add(1);
    } else {
      c.exact.fetch_add(1);
    }
    if (diverged) c.diverged.fetch_add(1);

    std::lock_guard<std::mutex> lk(lat_mu);
    source_lat_sec.push_back(source_lat);
    replay_lat_sec.push_back(replay_lat);
  }

  void RecordScheduleLag(double lag) {
    std::lock_guard<std::mutex> lk(lat_mu);
    schedule_lag_sec.push_back(lag);
  }
};

double Percentile(std::vector<double> v, double p) {
  if (v.empty()) return 0.0;
  std::sort(v.begin(), v.end());
  size_t idx = static_cast<size_t>(p * static_cast<double>(v.size() - 1));
  return v[idx];
}

// ---------------------------------------------------------------------------
// Minimal fixed-size thread pool with an unbounded FIFO queue. Bounding the
// worker count to `max_inflight` naturally caps concurrent *execution*;
// arrivals beyond that just wait in the queue, which is exactly the
// "schedule lag" the caller measures at dequeue time.
// ---------------------------------------------------------------------------

class ThreadPool {
 public:
  explicit ThreadPool(int n) {
    workers_.reserve(n);
    for (int i = 0; i < n; i++) workers_.emplace_back([this] { WorkerLoop(); });
  }

  void Submit(std::function<void()> task) {
    {
      std::lock_guard<std::mutex> lk(mu_);
      queue_.push(std::move(task));
    }
    cv_.notify_one();
  }

  // Signals that no more tasks will be submitted, then blocks until every
  // already-queued task has finished executing.
  void ShutdownAndJoin() {
    {
      std::lock_guard<std::mutex> lk(mu_);
      stopped_ = true;
    }
    cv_.notify_all();
    for (auto& w : workers_) {
      if (w.joinable()) w.join();
    }
  }

 private:
  void WorkerLoop() {
    for (;;) {
      std::function<void()> task;
      {
        std::unique_lock<std::mutex> lk(mu_);
        cv_.wait(lk, [this] { return stopped_ || !queue_.empty(); });
        if (queue_.empty()) {
          if (stopped_) return;
          continue;
        }
        task = std::move(queue_.front());
        queue_.pop();
      }
      task();
    }
  }

  std::mutex mu_;
  std::condition_variable cv_;
  std::queue<std::function<void()>> queue_;
  bool stopped_ = false;
  std::vector<std::thread> workers_;
};

// ---------------------------------------------------------------------------
// Peak overlap sweep, used as the default max_inflight.
// ---------------------------------------------------------------------------

int ComputePeakOverlap(const std::vector<EngineRecord>& records) {
  std::vector<std::pair<double, int>> events;
  events.reserve(records.size() * 2);
  for (const auto& r : records) {
    double start = r.p.start_time_sec;
    double end = start + r.p.duration_sec;
    events.emplace_back(start, 1);
    events.emplace_back(end, -1);
  }
  std::sort(events.begin(), events.end(), [](const auto& a, const auto& b) {
    if (a.first != b.first) return a.first < b.first;
    return a.second < b.second;  // process ends before starts at same instant
  });
  int cur = 0;
  int peak = 0;
  for (const auto& ev : events) {
    cur += ev.second;
    if (cur > peak) peak = cur;
  }
  return peak;
}

// ---------------------------------------------------------------------------
// Pre-scan: single-threaded, in sorted (reconstructed start time) order.
// Resolves each record's dependencies to producer record indices, marks
// skip/approximate classifications, and allocates completion signals for
// producer-capable records.
// ---------------------------------------------------------------------------

void PreScan(std::vector<EngineRecord>* records_ptr, AnomalyReport* report) {
  auto& records = *records_ptr;
  std::unordered_map<uint64_t, int64_t> ino_producer;
  std::unordered_map<uint64_t, int64_t> fh_producer;
  ino_producer[dingofs::kRootIno] = kRootSentinel;

  for (size_t i = 0; i < records.size(); i++) {
    EngineRecord& r = records[i];
    DepSpec d = ComputeDepSpec(r.p);
    r.num_ino_dep = d.num_ino;
    r.num_fh_dep = d.num_fh;

    bool missing = false;
    for (int k = 0; k < d.num_ino && !missing; k++) {
      auto it = ino_producer.find(d.ino[k]);
      if (it == ino_producer.end()) {
        r.skip = true;
        r.skip_reason =
            fmt::format("no earlier producer for source ino {}", d.ino[k]);
        missing = true;
      } else {
        r.ino_dep[k] = it->second;
      }
    }
    for (int k = 0; k < d.num_fh && !missing; k++) {
      auto it = fh_producer.find(d.fh[k]);
      if (it == fh_producer.end()) {
        r.skip = true;
        r.skip_reason =
            fmt::format("no earlier producer for source fh {}", d.fh[k]);
        missing = true;
      } else {
        r.fh_dep[k] = it->second;
      }
    }

    if (!missing) {
      if (r.p.op == OpKind::kCreate && !r.p.ok) {
        r.skip = true;
        r.skip_reason =
            "source create failed without a valid result attr; cannot "
            "approximate mode";
      } else if (r.p.op == OpKind::kSetAttr && !r.p.ok) {
        r.skip = true;
        r.skip_reason =
            "source setattr failed; input attribute values are not logged";
      }
    }

    if (r.skip) {
      report->Write(r.file, r.line, "skipped", r.skip_reason);
    } else {
      switch (r.p.op) {
        case OpKind::kCreate:
          r.approximate = true;
          r.approximate_reason =
              "mode reconstructed from result attr; flags assumed O_RDWR "
              "(source flags not logged)";
          break;
        case OpKind::kMkNod:
          r.approximate = true;
          r.approximate_reason = "dev assumed 0 (not logged)";
          break;
        case OpKind::kWrite:
          r.approximate = true;
          r.approximate_reason =
              "write buffer replaced with zero-filled data of the same "
              "size (content not logged)";
          break;
        default:
          break;
      }
      if (r.approximate) {
        report->Write(r.file, r.line, "approximate", r.approximate_reason);
      }

      if (r.p.has_result_ino)
        ino_producer[r.p.result_ino] = static_cast<int64_t>(i);
      if (r.p.has_result_fh)
        fh_producer[r.p.result_fh] = static_cast<int64_t>(i);
    }

    // The source fh lifecycle ends here regardless of whether this record
    // itself will be replayed, so later source fh reuse must not resolve to
    // the stale producer.
    if (r.p.op == OpKind::kRelease || r.p.op == OpKind::kReleaseDir) {
      fh_producer.erase(r.p.fh);
    }

    if (!r.skip && (r.p.has_result_ino || r.p.has_result_fh)) {
      r.done_promise = std::make_unique<std::promise<void>>();
      r.done_future = r.done_promise->get_future().share();
    }
  }
}

// ---------------------------------------------------------------------------
// Replay execution
// ---------------------------------------------------------------------------

std::string StatusTypeName(const Status& s) {
  std::string full = s.ToString();
  size_t i = 0;
  while (i < full.size() &&
         (std::isalpha(static_cast<unsigned char>(full[i])))) {
    i++;
  }
  return full.substr(0, i);
}

// Waits for the producer at `dep` and resolves the target ino it produced.
// Returns false if `dep` has no valid producer output (producer itself was
// skipped mid-flight or its replay call failed).
bool ResolveIno(std::vector<EngineRecord>& records, int64_t dep, Ino* out) {
  if (dep == kRootSentinel) {
    *out = dingofs::kRootIno;
    return true;
  }
  EngineRecord& prod = records[static_cast<size_t>(dep)];
  prod.done_future.wait();
  if (!prod.produced_ino) return false;
  *out = prod.target_ino;
  return true;
}

bool ResolveFh(std::vector<EngineRecord>& records, int64_t dep, uint64_t* out) {
  EngineRecord& prod = records[static_cast<size_t>(dep)];
  prod.done_future.wait();
  if (!prod.produced_fh) return false;
  *out = prod.target_fh;
  return true;
}

Status ExecuteOp(VFSWrapper* vfs, EngineRecord* rec, const Context& ctx,
                 Ino t_ino1, Ino t_ino2, uint64_t t_fh1, uint64_t t_fh2) {
  const ParsedRecord& p = rec->p;
  Status s;
  switch (p.op) {
    case OpKind::kLookup: {
      Attr attr;
      s = vfs->Lookup(ctx, t_ino1, p.name1, &attr);
      if (s.ok()) {
        rec->target_ino = attr.ino;
        rec->produced_ino = true;
      }
      break;
    }
    case OpKind::kGetAttr: {
      Attr attr;
      s = vfs->GetAttr(ctx, t_ino1, &attr);
      break;
    }
    case OpKind::kSetAttr: {
      Attr in_attr;
      Attr out_attr;
      if (p.set & kFuseSetAttrMode) in_attr.mode = p.r_mode;
      if (p.set & kFuseSetAttrUid) in_attr.uid = p.r_uid;
      if (p.set & kFuseSetAttrGid) in_attr.gid = p.r_gid;
      if (p.set & kFuseSetAttrSize) in_attr.length = p.r_length;
      if (p.set & (kFuseSetAttrAtime | kFuseSetAttrAtimeNow)) {
        in_attr.atime = p.r_atime;
      }
      if (p.set & (kFuseSetAttrMtime | kFuseSetAttrMtimeNow)) {
        in_attr.mtime = p.r_mtime;
      }
      s = vfs->SetAttr(ctx, t_ino1, p.set, in_attr, &out_attr);
      break;
    }
    case OpKind::kFallocate:
      s = vfs->Fallocate(ctx, t_ino1, p.flags, p.offset, p.length);
      break;
    case OpKind::kCopyFileRange: {
      uint64_t copied = 0;
      s = vfs->CopyFileRange(ctx, t_ino1, p.src_off, t_fh1, t_ino2, p.dst_off,
                             t_fh2, p.length, p.cp_flags, &copied);
      break;
    }
    case OpKind::kReadLink: {
      std::string link;
      s = vfs->ReadLink(ctx, t_ino1, &link);
      break;
    }
    case OpKind::kMkNod: {
      Attr attr;
      s = vfs->MkNod(ctx, t_ino1, p.name1, p.mode, /*dev=*/0, &attr);
      if (s.ok()) {
        rec->target_ino = attr.ino;
        rec->produced_ino = true;
      }
      break;
    }
    case OpKind::kUnlink:
      s = vfs->Unlink(ctx, t_ino1, p.name1);
      break;
    case OpKind::kSymlink: {
      Attr attr;
      s = vfs->Symlink(ctx, t_ino1, p.name1, p.name2, &attr);
      if (s.ok()) {
        rec->target_ino = attr.ino;
        rec->produced_ino = true;
      }
      break;
    }
    case OpKind::kRename:
      s = vfs->Rename(ctx, t_ino1, p.name1, t_ino2, p.name2);
      break;
    case OpKind::kLink: {
      Attr attr;
      s = vfs->Link(ctx, t_ino1, t_ino2, p.name1, &attr);
      if (s.ok()) {
        rec->target_ino = attr.ino;
        rec->produced_ino = true;
      }
      break;
    }
    case OpKind::kOpen: {
      uint64_t fh = 0;
      s = vfs->Open(ctx, t_ino1, p.flags, &fh);
      if (s.ok()) {
        rec->target_fh = fh;
        rec->produced_fh = true;
      }
      break;
    }
    case OpKind::kCreate: {
      uint64_t fh = 0;
      Attr attr;
      s = vfs->Create(ctx, t_ino1, p.name1, p.mode, O_RDWR, &fh, &attr);
      if (s.ok()) {
        rec->target_ino = attr.ino;
        rec->produced_ino = true;
        rec->target_fh = fh;
        rec->produced_fh = true;
      }
      break;
    }
    case OpKind::kRead: {
      DataBuffer buf;
      uint64_t rsize = 0;
      s = vfs->Read(ctx, t_ino1, &buf, p.size, p.offset, t_fh1, &rsize);
      break;
    }
    case OpKind::kWrite: {
      std::vector<char> buf(p.size, 0);  // approximate: content not logged
      uint64_t wsize = 0;
      s = vfs->Write(ctx, t_ino1, buf.data(), p.size, p.offset, t_fh1, &wsize);
      break;
    }
    case OpKind::kFlush:
      s = vfs->Flush(ctx, t_ino1, t_fh1);
      break;
    case OpKind::kRelease:
      s = vfs->Release(ctx, t_ino1, t_fh1);
      break;
    case OpKind::kFsync:
      s = vfs->Fsync(ctx, t_ino1, p.datasync, t_fh1);
      break;
    case OpKind::kGetXattr: {
      std::string value;
      s = vfs->GetXattr(ctx, t_ino1, p.name1, &value);
      break;
    }
    case OpKind::kRemoveXattr:
      s = vfs->RemoveXattr(ctx, t_ino1, p.name1);
      break;
    case OpKind::kListXattr: {
      std::vector<std::string> xattrs;
      s = vfs->ListXattr(ctx, t_ino1, &xattrs);
      break;
    }
    case OpKind::kMkDir: {
      Attr attr;
      s = vfs->MkDir(ctx, t_ino1, p.name1, p.mode, &attr);
      if (s.ok()) {
        rec->target_ino = attr.ino;
        rec->produced_ino = true;
      }
      break;
    }
    case OpKind::kOpenDir: {
      uint64_t fh = 0;
      bool need_cache = false;
      s = vfs->OpenDir(ctx, t_ino1, &fh, need_cache);
      if (s.ok()) {
        rec->target_fh = fh;
        rec->produced_fh = true;
      }
      break;
    }
    case OpKind::kReadDir: {
      auto handler = [](const DirEntry&, uint64_t) { return true; };
      s = vfs->ReadDir(ctx, t_ino1, t_fh1, p.offset, p.with_attr, handler);
      break;
    }
    case OpKind::kReleaseDir:
      s = vfs->ReleaseDir(ctx, t_ino1, t_fh1);
      break;
    case OpKind::kRmDir:
      s = vfs->RmDir(ctx, t_ino1, p.name1);
      break;
    case OpKind::kStatFs: {
      FsStat st;
      s = vfs->StatFs(ctx, t_ino1, &st);
      break;
    }
    default:
      s = Status::InvalidParam("unsupported op reached ExecuteOp");
      break;
  }
  return s;
}

void ExecuteRecord(VFSWrapper* vfs, std::vector<EngineRecord>* records_ptr,
                   size_t idx, Stats* stats, AnomalyReport* report,
                   std::chrono::steady_clock::time_point t0) {
  auto& records = *records_ptr;
  EngineRecord& r = records[idx];

  double now =
      std::chrono::duration<double>(std::chrono::steady_clock::now() - t0)
          .count();
  double lag = now - r.schedule_target_sec;
  r.schedule_lag_sec = lag;
  stats->RecordScheduleLag(lag);

  if (r.skip) {
    stats->CountSkip(r.p.op);
    return;
  }

  Ino t_ino1 = 0, t_ino2 = 0;
  uint64_t t_fh1 = 0, t_fh2 = 0;
  bool deps_ok = true;
  std::string dep_fail_reason;
  if (deps_ok && r.num_ino_dep >= 1 &&
      !ResolveIno(records, r.ino_dep[0], &t_ino1)) {
    deps_ok = false;
    dep_fail_reason = "producer for ino1 failed to replay";
  }
  if (deps_ok && r.num_ino_dep >= 2 &&
      !ResolveIno(records, r.ino_dep[1], &t_ino2)) {
    deps_ok = false;
    dep_fail_reason = "producer for ino2 failed to replay";
  }
  if (deps_ok && r.num_fh_dep >= 1 &&
      !ResolveFh(records, r.fh_dep[0], &t_fh1)) {
    deps_ok = false;
    dep_fail_reason = "producer for fh1 failed to replay";
  }
  if (deps_ok && r.num_fh_dep >= 2 &&
      !ResolveFh(records, r.fh_dep[1], &t_fh2)) {
    deps_ok = false;
    dep_fail_reason = "producer for fh2 failed to replay";
  }

  if (!deps_ok) {
    r.skip = true;
    r.skip_reason = dep_fail_reason;
    stats->CountSkip(r.p.op);
    report->Write(r.file, r.line, "skipped", r.skip_reason);
    if (r.done_promise) r.done_promise->set_value();
    return;
  }

  Context ctx{r.p.uid, r.p.gid, r.p.pid, /*umask=*/0};
  auto t_start = std::chrono::steady_clock::now();
  Status s = ExecuteOp(vfs, &r, ctx, t_ino1, t_ino2, t_fh1, t_fh2);
  auto t_end = std::chrono::steady_clock::now();
  r.replay_latency_sec = std::chrono::duration<double>(t_end - t_start).count();
  r.replay_ok = s.ok();
  r.replay_status_type = StatusTypeName(s);

  bool diverged = (r.replay_status_type != r.p.status_type);
  stats->CountOutcome(r.p.op, r.approximate, diverged, r.p.duration_sec,
                      r.replay_latency_sec);
  if (diverged) {
    report->Write(r.file, r.line, "divergence",
                  fmt::format("source status={} replay status={}",
                              r.p.status_type, r.replay_status_type));
  }

  if (r.done_promise) r.done_promise->set_value();
}

void RunReplay(VFSWrapper* vfs, std::vector<EngineRecord>* records_ptr,
               int max_inflight, double speed, Stats* stats,
               AnomalyReport* report) {
  auto& records = *records_ptr;
  double first_start = records.front().p.start_time_sec;
  auto t0 = std::chrono::steady_clock::now();

  ThreadPool pool(max_inflight);
  for (size_t i = 0; i < records.size(); i++) {
    double rel_sec = (records[i].p.start_time_sec - first_start) / speed;
    records[i].schedule_target_sec = rel_sec;
    std::this_thread::sleep_until(t0 + std::chrono::duration<double>(rel_sec));
    pool.Submit([&records, i, vfs, stats, report, t0] {
      ExecuteRecord(vfs, &records, i, stats, report, t0);
    });
  }
  pool.ShutdownAndJoin();
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

struct RecordStat {
  uint64_t n_ignored{0};
  uint64_t n_control{0};
  uint64_t n_malformed{0};
  uint64_t n_unsupported{0};
};

void PrintSummary(const std::vector<EngineRecord>& records, const Stats& stats,
                  RecordStat& record_stat, int max_inflight) {
  std::cout << "\n=== dingo-replay summary ===\n";
  std::cout << "records parsed:      " << records.size() << "\n";
  std::cout << "lines ignored:       " << record_stat.n_ignored << "\n";
  std::cout << "control records:     " << record_stat.n_control << "\n";
  std::cout << "lines malformed:     " << record_stat.n_malformed << "\n";
  std::cout << "ops unsupported:     " << record_stat.n_unsupported << "\n";
  std::cout << "max inflight used:   " << max_inflight << "\n\n";

  std::cout << fmt::format("{:<14}{:>8}{:>13}{:>10}{:>12}\n", "op", "exact",
                           "approximate", "skipped", "diverged");
  int64_t total_exact = 0, total_approx = 0, total_skip = 0, total_div = 0;
  for (size_t i = 0; i < kNumOpKinds; i++) {
    OpKind op = static_cast<OpKind>(i);
    if (op == OpKind::kUnsupported) continue;
    const auto& c = stats.per_op[i];
    int64_t exact = c.exact.load();
    int64_t approx = c.approximate.load();
    int64_t skipped = c.skipped.load();
    int64_t diverged = c.diverged.load();
    if (exact + approx + skipped + diverged == 0) continue;
    std::cout << fmt::format("{:<14}{:>8}{:>13}{:>10}{:>12}\n", OpKindName(op),
                             exact, approx, skipped, diverged);
    total_exact += exact;
    total_approx += approx;
    total_skip += skipped;
    total_div += diverged;
  }
  std::cout << fmt::format("{:<14}{:>8}{:>13}{:>10}{:>12}\n", "TOTAL",
                           total_exact, total_approx, total_skip, total_div);

  auto print_lat = [](const char* label, std::vector<double> v) {
    std::cout << fmt::format(
        "{:<22} p50={:.6f}s p95={:.6f}s p99={:.6f}s (n={})\n", label,
        Percentile(v, 0.50), Percentile(v, 0.95), Percentile(v, 0.99),
        v.size());
  };
  std::cout << "\n";
  print_lat("source latency:", stats.source_lat_sec);
  print_lat("replay latency:", stats.replay_lat_sec);

  std::vector<double> lag = stats.schedule_lag_sec;
  std::vector<double> positive_lag;
  positive_lag.reserve(lag.size());
  for (double l : lag) {
    if (l > 0) positive_lag.push_back(l);
  }
  std::cout << fmt::format(
      "schedule lag:          p50={:.6f}s p95={:.6f}s p99={:.6f}s "
      "(delayed={}/{})\n",
      Percentile(lag, 0.50), Percentile(lag, 0.95), Percentile(lag, 0.99),
      positive_lag.size(), lag.size());
}

std::vector<EngineRecord> ReadLogFiles(
    const std::vector<std::string>& log_files, AnomalyReport& report,
    RecordStat& out_stats) {
  std::vector<EngineRecord> records;
  uint64_t seq = 0;

  for (const std::string& file : log_files) {
    std::ifstream in(file);
    if (!in) {
      std::cerr << "Error: cannot open " << file << "\n";
      return {};
    }
    std::string line;
    size_t lineno = 0;
    while (std::getline(in, line)) {
      lineno++;
      auto res = dingofs::tools::replay::ParseAccessLogLine(line);
      using LS = dingofs::tools::replay::LineParseStatus;
      if (res.status == LS::kIgnoredOther) {
        out_stats.n_ignored++;
      } else if (res.status == LS::kControlRecord) {
        out_stats.n_control++;
      } else if (res.status == LS::kMalformed) {
        out_stats.n_malformed++;
        report.Write(file, lineno, "malformed", res.reason);
      } else {  // kOk
        if (res.record.op == dingofs::tools::replay::OpKind::kUnsupported) {
          out_stats.n_unsupported++;
          report.Write(file, lineno, "skipped",
                       "unsupported op (setxattr/ioctl): critical input not "
                       "captured by the access log");
          continue;
        }
        res.record.seq = seq++;
        dingofs::tools::replay::EngineRecord er;
        er.p = std::move(res.record);
        er.file = file;
        er.line = lineno;
        records.push_back(std::move(er));
      }
    }
  }

  return records;
}

}  // namespace
}  // namespace replay
}  // namespace tools
}  // namespace dingofs

int main(int argc, char** argv) {
  gflags::SetUsageMessage(
      "dingo-replay: best-effort semantic replay of a legacy fuse_access "
      "log against an empty target DingoFS filesystem\n"
      "Usage: dingo-replay --replay_mds_addr=... --replay_fs_name=... "
      "<log_file> [log_file ...]\n"
      "       dingo-replay --replay_self_check");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_replay_self_check) {
    bool ok = dingofs::tools::replay::RunParserSelfCheck(std::cout);
    return ok ? 0 : 1;
  }

  std::vector<std::string> log_files;
  for (int i = 1; i < argc; i++) log_files.emplace_back(argv[i]);

  if (!FLAGS_replay_conf.empty()) {
    if (!gflags::ReadFromFlagsFile(FLAGS_replay_conf, argv[0], true)) {
      std::cerr << "Failed to load config " << FLAGS_replay_conf << "\n";
      return 1;
    }
  }

  if (FLAGS_replay_mds_addr.empty() || FLAGS_replay_fs_name.empty()) {
    std::cerr << "Error: --replay_mds_addr and --replay_fs_name are required\n";
    return 1;
  }
  if (log_files.empty()) {
    std::cerr << "Error: no input log files given (positional arguments)\n";
    return 1;
  }
  if (FLAGS_replay_speed <= 0) {
    std::cerr << "Error: --replay_speed must be > 0\n";
    return 1;
  }

  FLAGS_log_dir = FLAGS_replay_log_dir;
  dingofs::FLAGS_log_level = FLAGS_replay_log_level;
  dingofs::Logger::Init("dingo-replay");

  dingofs::tools::replay::AnomalyReport report(FLAGS_replay_report);

  dingofs::tools::replay::RecordStat record_stat;
  // ---- Phase 1: parse all input lines. ----
  std::vector<dingofs::tools::replay::EngineRecord> records =
      ReadLogFiles(log_files, report, record_stat);
  if (records.empty()) {
    std::cerr << "Error: no replayable records parsed from input logs\n";
    return 1;
  }

  std::stable_sort(records.begin(), records.end(),  // NOLINT
                   [](const dingofs::tools::replay::EngineRecord& a,
                      const dingofs::tools::replay::EngineRecord& b) {
                     return a.p.start_time_sec < b.p.start_time_sec;
                   });

  int max_inflight = FLAGS_replay_max_inflight;
  if (max_inflight <= 0) {
    max_inflight = dingofs::tools::replay::ComputePeakOverlap(records);
    max_inflight = std::max(max_inflight, 1);
  }

  dingofs::tools::replay::PreScan(&records, &report);

  auto vfs = std::make_unique<dingofs::client::VFSWrapper>();
  dingofs::client::DingofsConfig config;
  config.mds_addrs = FLAGS_replay_mds_addr;
  config.fs_name = FLAGS_replay_fs_name;
  config.mount_point = FLAGS_replay_mount_point;
  config.metasystem_type =
      dingofs::MetaSystemTypeToString(dingofs::MetaSystemType::MDS);
  config.storage_info = "";
  config.subdir = "/";

  std::cout << "Mounting " << FLAGS_replay_mds_addr << "/"
            << FLAGS_replay_fs_name << " ...\n";
  dingofs::Status s = vfs->Start(config);
  if (!s.ok()) {
    std::cerr << "Mount failed: " << s.ToString() << "\n";
    return 1;
  }

  std::cout << "Replaying " << records.size()
            << " records (max_inflight=" << max_inflight
            << ", speed=" << FLAGS_replay_speed << ") ...\n";

  dingofs::tools::replay::Stats stats;
  dingofs::tools::replay::RunReplay(vfs.get(), &records, max_inflight,
                                    FLAGS_replay_speed, &stats, &report);

  vfs->Stop(false);

  dingofs::tools::replay::PrintSummary(records, stats, record_stat,
                                       max_inflight);
  return 0;
}
