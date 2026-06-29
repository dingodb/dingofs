// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "tools/mds-cli/dir_tree_walker.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "dingofs/error.pb.h"
#include "dingofs/mds.pb.h"

namespace dingofs {
namespace mds {
namespace client {

namespace {

using pb::mds::FileType;

// ReadDir is served one page at a time; this value is passed as the request
// limit, so a page with fewer entries is the last one.
constexpr int kReadDirPage = 100;
// ListDentry page size for sub-directory enumeration.
constexpr uint32_t kListDentryPage = 1000;

bool IsOk(const pb::error::Error& e) {
  return e.errcode() == pb::error::Errno::OK;
}
bool IsNotFound(const pb::error::Error& e) {
  return e.errcode() == pb::error::Errno::ENOT_FOUND;
}

// One directory's single-level contribution plus its sub-directories.
struct LevelResult {
  bool ok{false};        // read succeeded; files/length/subdirs are valid
  bool vanished{false};  // ENOT_FOUND (concurrent rmdir) -> skip, not an error
  int64_t files{0};      // direct non-directory children (symlinks included)
  int64_t length{0};     // sum of direct FILE child lengths
  std::vector<std::pair<Ino, std::string>> subdirs;  // {ino, name}
};

// Page through ListDentry(is_only_dir=true) on `owner`, collecting
// sub-directory {ino, name}. Returns false on failure; `vanished` distinguishes
// ENOT_FOUND.
bool ListSubdirs(MDSClient* owner, Ino ino,
                 std::vector<std::pair<Ino, std::string>>& out,
                 bool& vanished) {
  vanished = false;
  std::string last;
  for (;;) {
    auto resp = owner->ListDentryPaged(ino, last, kListDentryPage,
                                       /*is_only_dir=*/true);
    if (!IsOk(resp.error())) {
      vanished = IsNotFound(resp.error());
      return false;
    }
    const int n = resp.dentries_size();
    for (const auto& d : resp.dentries()) {
      // The store-backed ListDentry scans inclusive of `last`; drop the
      // repeated boundary entry so it is not counted twice (names are unique in
      // a dir).
      if (!last.empty() && d.name() == last) continue;
      out.emplace_back(d.ino(), d.name());
    }
    if (n < static_cast<int>(kListDentryPage)) break;
    last = resp.dentries(n - 1).name();
  }
  return true;
}

// Read one directory level (routed to `ino`'s owner): its single-level
// {files, length} and the list of sub-directories to recurse into.
LevelResult ReadLevel(OwnerRouter& router, Ino ino, const WalkOptions& opts) {
  LevelResult lr;
  MDSClient* owner = router.ClientForIno(ino);
  if (owner == nullptr) {
    LOG(ERROR) << fmt::format(
        "no owner mds for ino({}); subtree skipped, result incomplete", ino);
    return lr;  // ok=false, vanished=false -> incomplete
  }

  const bool strict = opts.strict || !opts.dirstats_enabled;
  if (!strict) {
    // FAST: trust the stored single-level stat (the owner folds its own
    // unflushed delta), and only enumerate sub-directories when there are any.
    auto resp = owner->GetDirStat(ino);
    if (!IsOk(resp.error())) {
      lr.vanished = IsNotFound(resp.error());
      if (!lr.vanished)
        LOG(ERROR) << fmt::format("GetDirStat ino({}) fail: {}", ino,
                                  resp.error().errmsg());
      return lr;
    }
    const auto& ds = resp.dir_stat();
    const int64_t dirs = ds.dirs();
    lr.files = ds.inodes() - dirs;
    lr.length = ds.length();
    if (dirs > 0 && !ListSubdirs(owner, ino, lr.subdirs, lr.vanished)) {
      if (!lr.vanished)
        LOG(ERROR) << fmt::format("ListDentry ino({}) fail; result incomplete",
                                  ino);
      return lr;
    }
    lr.ok = true;
    return lr;
  }

  // STRICT: authoritative dentry scan. The dentry type is the source of truth
  // and file lengths come from the inode attrs in the same response.
  std::string last;
  for (;;) {
    auto resp = owner->ReadDir(ino, last, /*with_attr=*/true,
                               /*is_refresh=*/false, kReadDirPage);
    if (!IsOk(resp.error())) {
      lr.vanished = IsNotFound(resp.error());
      if (!lr.vanished)
        LOG(ERROR) << fmt::format("ReadDir ino({}) fail; result incomplete",
                                  ino);
      return lr;
    }
    const int n = resp.entries_size();
    for (const auto& e : resp.entries()) {
      if (!last.empty() && e.name() == last)
        continue;  // boundary dedup (see ListSubdirs)
      if (e.inode().type() == FileType::DIRECTORY) {
        lr.subdirs.emplace_back(e.ino(), e.name());
      } else {
        ++lr.files;
        if (e.inode().type() == FileType::FILE)
          lr.length += static_cast<int64_t>(e.inode().length());
      }
    }
    if (n < kReadDirPage) break;
    last = resp.entries(n - 1).name();
  }
  lr.ok = true;
  return lr;
}

// Push-based parallel directory walk. `visit(ino)` does the per-directory work
// (synchronizing its own aggregation) and returns the child directory inos to
// schedule. A worker never blocks on its children (no parent->child join), so a
// fixed pool cannot deadlock; the walk ends when the queue drains with no task
// in flight.
template <typename Visit>
void ParallelWalk(Ino root, uint32_t threads, Visit visit) {
  if (threads < 1) threads = 1;

  std::mutex mu;
  std::condition_variable cv;
  std::deque<Ino> queue;
  int64_t active = 0;
  bool stop = false;
  queue.push_back(root);

  auto worker = [&]() {
    for (;;) {
      Ino ino;
      {
        std::unique_lock<std::mutex> lk(mu);
        cv.wait(lk, [&] { return stop || !queue.empty(); });
        if (stop) return;
        ino = queue.front();
        queue.pop_front();
        ++active;
      }

      std::vector<Ino> children = visit(ino);

      {
        std::unique_lock<std::mutex> lk(mu);
        for (Ino c : children) queue.push_back(c);
        --active;
        if (queue.empty() && active == 0) stop = true;
      }
      cv.notify_all();
    }
  };

  std::vector<std::thread> workers;
  workers.reserve(threads);
  for (uint32_t i = 0; i < threads; ++i) workers.emplace_back(worker);
  for (auto& t : workers) t.join();
}

// Per-directory single-level data captured during a WalkTree traversal, used to
// build the TreeSummary offline (in-memory, single-threaded) afterwards.
struct NodeData {
  int64_t files{0};
  int64_t length{0};
  std::vector<std::pair<Ino, std::string>> subdirs;
};

// Build the TreeSummary node for `ino` from the captured `nodes`, returning its
// recursive {dirs, files, length}. Expands child nodes while `depth > 0`; at
// depth 0 it only aggregates (no further expansion). Each directory appears
// once (a tree has a single parent per node), so no memoization is needed.
DirAgg BuildNode(const std::unordered_map<Ino, NodeData>& nodes, Ino ino,
                 uint32_t depth, uint32_t top_n, pb::mds::TreeSummary* node) {
  auto it = nodes.find(ino);
  if (it == nodes.end()) {
    // Vanished or unreadable during the walk: an empty subtree.
    if (node != nullptr) {
      node->set_dirs(0);
      node->set_files(0);
      node->set_length(0);
    }
    return {};
  }

  const NodeData& nd = it->second;
  DirAgg agg{nd.files, /*dirs=*/1, nd.length};  // count this directory itself
  for (const auto& [cino, cname] : nd.subdirs) {
    if (depth > 0) {
      auto* child = node->add_children();
      child->set_ino(cino);
      child->set_path(cname);
      child->set_type(FileType::DIRECTORY);
      DirAgg c = BuildNode(nodes, cino, depth - 1, top_n, child);
      agg.files += c.files;
      agg.dirs += c.dirs;
      agg.length += c.length;
    } else {
      DirAgg c = BuildNode(nodes, cino, 0, top_n, nullptr);
      agg.files += c.files;
      agg.dirs += c.dirs;
      agg.length += c.length;
    }
  }

  if (node != nullptr) {
    node->set_files(agg.files);
    node->set_dirs(agg.dirs);
    node->set_length(agg.length);
    if (depth > 0) CollapseChildrenTopN(*node, top_n);
  }
  return agg;
}

}  // namespace

bool WalkAggregate(OwnerRouter& router, Ino root, const WalkOptions& opts,
                   DirAgg& out) {
  std::atomic<int64_t> files{0};
  std::atomic<int64_t> dirs{0};
  std::atomic<int64_t> length{0};
  std::atomic<bool> incomplete{false};

  ParallelWalk(root, opts.threads, [&](Ino ino) -> std::vector<Ino> {
    LevelResult lr = ReadLevel(router, ino, opts);
    if (!lr.ok) {
      if (!lr.vanished) incomplete.store(true);
      return {};
    }
    dirs.fetch_add(1);
    files.fetch_add(lr.files);
    length.fetch_add(lr.length);

    std::vector<Ino> children;
    children.reserve(lr.subdirs.size());
    for (const auto& s : lr.subdirs) children.push_back(s.first);
    return children;
  });

  out.files = files.load();
  out.dirs = dirs.load();
  out.length = length.load();
  return !incomplete.load();
}

bool ReadDirStatStrict(OwnerRouter& router, Ino ino, DirAgg& out) {
  out = DirAgg{};
  WalkOptions opts;
  opts.strict = true;  // force the authoritative dentry scan
  LevelResult lr = ReadLevel(router, ino, opts);
  if (!lr.ok) return false;  // no owner / read error / ENOT_FOUND
  out.files = lr.files;
  out.dirs = static_cast<int64_t>(lr.subdirs.size());  // direct sub-dir count
  out.length = lr.length;
  return true;
}

bool ReadDirStatFast(OwnerRouter& router, Ino ino, DirAgg& out) {
  out = DirAgg{};
  MDSClient* owner = router.ClientForIno(ino);
  if (owner == nullptr) {
    LOG(ERROR) << fmt::format("no owner mds for ino({})", ino);
    return false;
  }
  auto resp = owner->GetDirStat(ino);
  if (!IsOk(resp.error())) {
    if (!IsNotFound(resp.error()))
      LOG(ERROR) << fmt::format("GetDirStat ino({}) fail: {}", ino,
                                resp.error().errmsg());
    return false;
  }
  const auto& ds = resp.dir_stat();
  const int64_t dirs = ds.dirs();
  out.files = ds.inodes() - dirs;
  out.dirs = dirs;
  out.length = ds.length();
  return true;
}

bool WalkTree(OwnerRouter& router, Ino root, const WalkOptions& opts,
              uint32_t depth, uint32_t top_n, pb::mds::TreeSummary& out) {
  std::mutex map_mu;
  std::unordered_map<Ino, NodeData> nodes;
  std::atomic<bool> incomplete{false};

  // One parallel pass over the whole subtree captures every directory's
  // single-level data; the structured tree is then built offline.
  ParallelWalk(root, opts.threads, [&](Ino ino) -> std::vector<Ino> {
    LevelResult lr = ReadLevel(router, ino, opts);
    if (!lr.ok) {
      if (!lr.vanished) incomplete.store(true);
      return {};
    }
    std::vector<Ino> children;
    children.reserve(lr.subdirs.size());
    for (const auto& s : lr.subdirs) children.push_back(s.first);

    NodeData nd;
    nd.files = lr.files;
    nd.length = lr.length;
    nd.subdirs = std::move(lr.subdirs);
    {
      std::lock_guard<std::mutex> lg(map_mu);
      nodes[ino] = std::move(nd);
    }
    return children;
  });

  // A real read error means the caller aborts without printing, so skip building
  // a tree nobody will read. (Benign concurrent rmdir does not set `incomplete`;
  // that subtree is still built and rendered as an empty node.)
  if (incomplete.load()) return false;

  out.set_ino(root);
  out.set_type(FileType::DIRECTORY);
  // The root row's path is supplied by the renderer (base_path); leave it
  // empty.
  BuildNode(nodes, root, depth, top_n, &out);
  return true;
}

bool WalkSyncDirStat(OwnerRouter& router, Ino root, const WalkOptions& opts,
                     bool repair,
                     std::vector<pb::mds::DirStatMismatch>& mismatches) {
  std::mutex mismatches_mu;
  std::atomic<bool> incomplete{false};

  ParallelWalk(root, opts.threads, [&](Ino ino) -> std::vector<Ino> {
    MDSClient* owner = router.ClientForIno(ino);
    if (owner == nullptr) {
      LOG(ERROR) << fmt::format(
          "no owner mds for ino({}); subtree skipped, result incomplete", ino);
      incomplete.store(true);
      return {};
    }

    // Single-level check/repair for this directory.
    auto resp = owner->SyncDirStat(ino, repair);
    if (IsOk(resp.error())) {
      if (resp.mismatches_size() > 0) {
        std::lock_guard<std::mutex> lg(mismatches_mu);
        for (const auto& b : resp.mismatches()) mismatches.push_back(b);
      }
    } else if (!IsNotFound(resp.error())) {
      LOG(ERROR) << fmt::format(
          "SyncDirStat ino({}) fail: {}; result incomplete", ino,
          resp.error().errmsg());
      incomplete.store(true);
    }

    // Enumerate sub-directories (dentry truth) to recurse into.
    std::vector<std::pair<Ino, std::string>> subdirs;
    bool vanished = false;
    if (!ListSubdirs(owner, ino, subdirs, vanished)) {
      if (!vanished) {
        LOG(ERROR) << fmt::format("ListDentry ino({}) fail; result incomplete",
                                  ino);
        incomplete.store(true);
      }
      return {};
    }
    std::vector<Ino> children;
    children.reserve(subdirs.size());
    for (const auto& s : subdirs) children.push_back(s.first);
    return children;
  });

  std::sort(mismatches.begin(), mismatches.end(),
            [](const pb::mds::DirStatMismatch& a,
               const pb::mds::DirStatMismatch& b) {
              return a.ino() < b.ino();
            });
  return !incomplete.load();
}

void CollapseChildrenTopN(pb::mds::TreeSummary& node, uint32_t top_n) {
  auto* children = node.mutable_children();
  std::sort(children->begin(), children->end(),
            [](const pb::mds::TreeSummary& a, const pb::mds::TreeSummary& b) {
              return a.length() > b.length();
            });

  if (top_n == 0 || static_cast<uint32_t>(children->size()) <= top_n) return;

  // Merge the omitted (lower-ranked) sub-directories into a single "..." node.
  pb::mds::TreeSummary omit;
  omit.set_path("...");
  omit.set_type(FileType::FILE);
  for (int i = static_cast<int>(top_n); i < children->size(); ++i) {
    const auto& c = children->Get(i);
    omit.set_length(omit.length() + c.length());
    omit.set_files(omit.files() + c.files());
    omit.set_dirs(omit.dirs() + c.dirs());
  }
  children->DeleteSubrange(static_cast<int>(top_n),
                           children->size() - static_cast<int>(top_n));
  *children->Add() = omit;
}

}  // namespace client
}  // namespace mds
}  // namespace dingofs
