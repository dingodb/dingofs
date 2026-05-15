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

#include "mds/client/trash_restore.h"

#include <fmt/format.h>
#include <glog/logging.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "common/meta.h"
#include "dingofs/error.pb.h"
#include "mds/common/trash.h"

namespace dingofs {
namespace mds {
namespace client {

namespace {

// ListDentry batch size per RPC. 1024 matches the default used by other MDS
// callers; the server returns up to this many dentries per call.
constexpr uint32_t kListDentryBatch = 1024;

// Caller uid passed through to the RestoreFromTrash RPC. The server enforces
// uid == 0; the CLI also refuses to run under a non-root euid, so this is
// simply the literal value we send.
constexpr uint32_t kRootUid = 0;

}  // namespace

bool TrashRestore::Init(const std::string& mds_addr, const Options& options) {
  if (geteuid() != 0) {
    std::cerr << "only root can restore files from trash\n";
    return false;
  }
  if (mds_addr.empty()) {
    std::cerr << "mds_addr is empty\n";
    return false;
  }
  if (options.fs_id == 0) {
    std::cerr << "fs_id is required\n";
    return false;
  }
  if (options.hours.empty()) {
    std::cerr << "at least one HOUR (YYYY-MM-DD-HH) must be provided via --hours\n";
    return false;
  }

  options_ = options;
  if (options_.threads == 0) options_.threads = 1;

  client_ = std::make_unique<MDSClient>(options_.fs_id);
  if (!client_->Init(mds_addr)) {
    std::cerr << "init mds client fail\n";
    return false;
  }
  return true;
}

void TrashRestore::Run() {
  for (const auto& hour : options_.hours) {
    DoRestoreHour(hour);
  }
}

void TrashRestore::DoRestoreHour(const std::string& hour) {
  // Validate HOUR format; skip (with warn) rather than abort the entire run.
  if (ParseTrashBucketName(hour) == 0) {
    LOG(ERROR) << fmt::format("invalid HOUR format '{}', expected YYYY-MM-DD-HH (UTC)", hour);
    return;
  }

  // Reset per-hour state.
  {
    std::lock_guard<std::mutex> lg(mu_);
    std::queue<pb::mds::Dentry>().swap(queue_);
    queue_closed_ = false;
    trashed_dir_inos_.clear();
  }
  restored_.store(0);
  skipped_.store(0);
  failed_.store(0);

  LOG(INFO) << fmt::format("restore trash in {} (put_back={}, threads={})", hour, options_.put_back, options_.threads);

  // .trash is synthesized client-side and has no dentry under kRootIno, so
  // we look up the hour bucket under kTrashInodeId directly.
  auto bucket_resp = client_->Lookup(kTrashInodeId, hour);
  if (bucket_resp.error().errcode() != pb::error::OK) {
    LOG(ERROR) << fmt::format("lookup .trash/{} fail: {} ({})", hour, bucket_resp.error().errmsg(),
                              static_cast<int>(bucket_resp.error().errcode()));
    return;
  }
  Ino bucket_ino = bucket_resp.inode().ino();

  std::vector<pb::mds::Dentry> entries;
  {
    auto resp = client_->ListDentry(bucket_ino, /*is_only_dir=*/false);
    if (resp.error().errcode() != pb::error::OK) {
      LOG(ERROR) << fmt::format("list .trash/{} fail: {} ({})", hour, resp.error().errmsg(),
                                static_cast<int>(resp.error().errcode()));
      return;
    }
    entries.reserve(resp.dentries_size());
    for (const auto& d : resp.dentries()) entries.push_back(d);
  }

  if (entries.empty()) {
    LOG(INFO) << fmt::format("no entries in .trash/{}", hour);
    return;
  }

  // Shuffle to reduce transaction conflicts on hot inodes across parallel
  // restore runs.
  std::mt19937_64 rng(std::chrono::steady_clock::now().time_since_epoch().count());
  std::shuffle(entries.begin(), entries.end(), rng);

  // In tree-rebuild mode, we only restore entries whose original parent was
  // also trashed (and therefore appears in this bucket as a directory). Build
  // that set before dispatching workers.
  if (!options_.put_back) {
    for (const auto& d : entries) {
      if (d.type() == pb::mds::FileType::DIRECTORY) {
        trashed_dir_inos_.insert(d.ino());
      }
    }
  }

  {
    std::lock_guard<std::mutex> lg(mu_);
    for (auto& d : entries) queue_.push(std::move(d));
    queue_closed_ = true;
  }
  cv_.notify_all();

  std::vector<std::thread> workers;
  workers.reserve(options_.threads);
  for (uint32_t i = 0; i < options_.threads; ++i) {
    workers.emplace_back([this, bucket_ino] { WorkerLoop(bucket_ino); });
  }
  for (auto& t : workers) t.join();

  LOG(INFO) << fmt::format("restored {} in {} (skipped={}, failed={})", restored_.load(), hour, skipped_.load(),
                           failed_.load());
}

void TrashRestore::WorkerLoop(Ino bucket_ino) {
  while (true) {
    pb::mds::Dentry dentry;
    {
      std::unique_lock<std::mutex> lk(mu_);
      cv_.wait(lk, [this] { return !queue_.empty() || queue_closed_; });
      if (queue_.empty()) return;
      dentry = std::move(queue_.front());
      queue_.pop();
    }

    // Parse the trash entry name to decide whether to skip (tree-rebuild)
    // and for logging. The server re-parses for correctness; the CLI-side
    // parse is informational only.
    Ino orig_parent = ParseTrashEntryName(dentry.name());
    if (orig_parent == 0) {
      LOG(WARNING) << fmt::format("skip unparseable trash entry '{}'", dentry.name());
      failed_.fetch_add(1);
      continue;
    }

    if (!options_.put_back && trashed_dir_inos_.count(orig_parent) == 0) {
      skipped_.fetch_add(1);
      continue;
    }

    const bool allow_trash_parent = !options_.put_back;
    auto resp = client_->RestoreFromTrash(bucket_ino, dentry.name(), kRootUid, allow_trash_parent);
    if (resp.error().errcode() == pb::error::OK) {
      restored_.fetch_add(1);
    } else {
      LOG(WARNING) << fmt::format("restore '{}' fail: {} ({})", dentry.name(), resp.error().errmsg(),
                                  static_cast<int>(resp.error().errcode()));
      failed_.fetch_add(1);
    }
  }
}

}  // namespace client
}  // namespace mds
}  // namespace dingofs
