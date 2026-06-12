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

#include "tools/mds-cli/trash_restore.h"

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
    std::cerr
        << "at least one HOUR (YYYY-MM-DD-HH) must be provided via --hours\n";
    return false;
  }

  options_ = options;
  if (options_.threads == 0) options_.threads = 1;

  client_ = std::make_unique<MDSClient>(options_.fs_id);
  if (!client_->Init(mds_addr)) {
    std::cerr << "init mds client fail\n";
    return false;
  }
  return InitRouting();
}

bool TrashRestore::InitRouting() {
  auto fs_resp = client_->GetFs(options_.fs_id);
  if (fs_resp.error().errcode() != pb::error::OK) {
    std::cerr << fmt::format("get fs info fail: {} ({})\n",
                             fs_resp.error().errmsg(),
                             static_cast<int>(fs_resp.error().errcode()));
    return false;
  }
  partition_policy_ = fs_resp.fs_info().partition_policy();

  auto mds_resp = client_->GetMdsList();
  if (mds_resp.error().errcode() != pb::error::OK) {
    std::cerr << fmt::format("get mds list fail: {} ({})\n",
                             mds_resp.error().errmsg(),
                             static_cast<int>(mds_resp.error().errcode()));
    return false;
  }
  std::unordered_map<uint64_t, std::string> mds_addrs;
  for (const auto& mds : mds_resp.mdses()) {
    mds_addrs[mds.id()] =
        fmt::format("{}:{}", mds.location().host(), mds.location().port());
  }

  std::unordered_set<uint64_t> owner_mds_ids;
  if (partition_policy_.type() ==
      pb::mds::PartitionType::MONOLITHIC_PARTITION) {
    owner_mds_ids.insert(partition_policy_.mono().mds_id());

  } else if (partition_policy_.type() ==
             pb::mds::PartitionType::PARENT_ID_HASH_PARTITION) {
    if (partition_policy_.parent_hash().bucket_num() == 0) {
      std::cerr << "parent hash bucket_num is 0\n";
      return false;
    }
    for (const auto& [mds_id, bucket_set] :
         partition_policy_.parent_hash().distributions()) {
      owner_mds_ids.insert(mds_id);
      for (const auto& bucket_id : bucket_set.bucket_ids())
        bucket_to_mds_[bucket_id] = mds_id;
    }

  } else {
    std::cerr << fmt::format(
        "unknown partition type({})\n",
        pb::mds::PartitionType_Name(partition_policy_.type()));
    return false;
  }

  for (uint64_t mds_id : owner_mds_ids) {
    auto it = mds_addrs.find(mds_id);
    if (it == mds_addrs.end()) {
      LOG(ERROR) << fmt::format(
          "mds({}) not in mds list, entries owned by it will fail to restore",
          mds_id);
      continue;
    }
    auto mds_client = std::make_unique<MDSClient>(options_.fs_id);
    if (!mds_client->Init(it->second)) {
      LOG(ERROR) << fmt::format(
          "init client for mds({}) at {} fail, entries owned by it will fail "
          "to restore",
          mds_id, it->second);
      continue;
    }
    mds_clients_[mds_id] = std::move(mds_client);
  }

  return true;
}

MDSClient* TrashRestore::ClientForParent(Ino parent) {
  uint64_t mds_id = 0;
  if (partition_policy_.type() ==
      pb::mds::PartitionType::MONOLITHIC_PARTITION) {
    mds_id = partition_policy_.mono().mds_id();

  } else {
    auto it = bucket_to_mds_.find(static_cast<uint32_t>(
        parent % partition_policy_.parent_hash().bucket_num()));
    if (it != bucket_to_mds_.end()) mds_id = it->second;
  }

  auto it = mds_clients_.find(mds_id);
  if (it != mds_clients_.end()) return it->second.get();

  return nullptr;
}

void TrashRestore::Run() {
  for (const auto& hour : options_.hours) {
    DoRestoreHour(hour);
  }
}

void TrashRestore::DoRestoreHour(const std::string& hour) {
  // Validate HOUR format; skip (with warn) rather than abort the entire run.
  if (ParseTrashBucketName(hour) == 0) {
    LOG(ERROR) << fmt::format(
        "invalid HOUR format '{}', expected YYYY-MM-DD-HH (UTC)", hour);
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

  LOG(INFO) << fmt::format("restore trash in {} (put_back={}, threads={})",
                           hour, options_.put_back, options_.threads);

  // .trash is synthesized client-side and has no dentry under kRootIno, so
  // we look up the hour bucket under kTrashInodeId directly.
  auto bucket_resp = client_->Lookup(kTrashInodeId, hour);
  if (bucket_resp.error().errcode() != pb::error::OK) {
    LOG(ERROR) << fmt::format("lookup .trash/{} fail: {} ({})", hour,
                              bucket_resp.error().errmsg(),
                              static_cast<int>(bucket_resp.error().errcode()));
    return;
  }
  Ino bucket_ino = bucket_resp.inode().ino();

  std::vector<pb::mds::Dentry> entries;
  {
    auto resp = client_->ListDentry(bucket_ino, /*is_only_dir=*/false);
    if (resp.error().errcode() != pb::error::OK) {
      LOG(ERROR) << fmt::format("list .trash/{} fail: {} ({})", hour,
                                resp.error().errmsg(),
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

  // Partition into directories vs files. Directories must be restored before
  // files that live under them: a file's RestoreFromTrash triggers
  // AsyncUpdateDirUsage(parent=its_dir_ino, ...). When the quota walk reads
  // the dir's inode, the dir's parents() must already be the live
  // [actual_dst_parent] (i.e. the dir has already been restored); otherwise
  // parents() is still [bucket_ino], the walk hits IsTrashInode and stops at
  // the trash boundary -- and the per-dir quota credit for the file is lost.
  // The same ordering matters between dirs themselves when restoring a
  // multi-level subtree (qt/a/b/c/...): c's restore would see b as still in
  // trash unless b is restored first, etc. Sorting dir entries ASCENDING by
  // their original parent ino (parsed out of the trash entry name) gives the
  // right topo order under DingoFS's monotonic ino allocation -- a parent
  // dir is always created before its children, so parent_ino < child_ino,
  // which means every ancestor sorts before its descendants. Numerical sort
  // is required: lex sort on the name would mis-order "99-..." vs "100-..."
  // (because '1' < '9').
  std::vector<pb::mds::Dentry> dirs;
  std::vector<pb::mds::Dentry> files;
  dirs.reserve(entries.size());
  files.reserve(entries.size());
  for (auto& d : entries) {
    if (d.type() == pb::mds::FileType::DIRECTORY) {
      dirs.push_back(std::move(d));
    } else {
      files.push_back(std::move(d));
    }
  }
  std::sort(dirs.begin(), dirs.end(),
            [](const pb::mds::Dentry& a, const pb::mds::Dentry& b) {
              return ParseTrashEntryName(a.name()) <
                     ParseTrashEntryName(b.name());
            });

  // In tree-rebuild mode, we only restore entries whose original parent was
  // also trashed (and therefore appears in this bucket as a directory). Build
  // that set before dispatching workers.
  if (!options_.put_back) {
    for (const auto& d : dirs) trashed_dir_inos_.insert(d.ino());
  }

  // Phase 1: restore directories serially.
  for (const auto& d : dirs) RestoreOne(bucket_ino, d);

  // Phase 2: restore files in parallel. Shuffle to spread txn conflicts.
  std::mt19937_64 rng(
      std::chrono::steady_clock::now().time_since_epoch().count());
  std::shuffle(files.begin(), files.end(), rng);

  {
    std::lock_guard<std::mutex> lg(mu_);
    for (auto& d : files) queue_.push(std::move(d));
    queue_closed_ = true;
  }
  cv_.notify_all();

  std::vector<std::thread> workers;
  workers.reserve(options_.threads);
  for (uint32_t i = 0; i < options_.threads; ++i) {
    workers.emplace_back([this, bucket_ino] { WorkerLoop(bucket_ino); });
  }
  for (auto& t : workers) t.join();

  LOG(INFO) << fmt::format("restored {} in {} (skipped={}, failed={})",
                           restored_.load(), hour, skipped_.load(),
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
    RestoreOne(bucket_ino, dentry);
  }
}

void TrashRestore::RestoreOne(Ino bucket_ino, const pb::mds::Dentry& dentry) {
  // Parse the trash entry name to decide whether to skip (tree-rebuild)
  // and for logging. The server re-parses for correctness; the CLI-side
  // parse is informational only.
  Ino orig_parent = ParseTrashEntryName(dentry.name());
  if (orig_parent == 0) {
    LOG(WARNING) << fmt::format("skip unparseable trash entry '{}'",
                                dentry.name());
    failed_.fetch_add(1);
    return;
  }

  if (!options_.put_back && trashed_dir_inos_.count(orig_parent) == 0) {
    skipped_.fetch_add(1);
    return;
  }

  const bool allow_trash_parent = !options_.put_back;
  // Route to the MDS owning orig_parent's partition so its inode/dentry
  // caches are updated first-hand. No route means no restore: sending the
  // request to another MDS would leave the owner serving stale caches.
  MDSClient* mds_client = ClientForParent(orig_parent);
  if (mds_client == nullptr) {
    LOG(ERROR) << fmt::format("no owner mds for parent({}), restore '{}' fail",
                              orig_parent, dentry.name());
    failed_.fetch_add(1);
    return;
  }
  auto resp = mds_client->RestoreFromTrash(bucket_ino, dentry.name(), kRootUid,
                                           allow_trash_parent);
  if (resp.error().errcode() == pb::error::OK) {
    restored_.fetch_add(1);
  } else {
    LOG(WARNING) << fmt::format("restore '{}' fail: {} ({})", dentry.name(),
                                resp.error().errmsg(),
                                static_cast<int>(resp.error().errcode()));
    failed_.fetch_add(1);
  }
}

}  // namespace client
}  // namespace mds
}  // namespace dingofs
