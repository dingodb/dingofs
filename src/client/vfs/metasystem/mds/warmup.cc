// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "client/vfs/metasystem/mds/warmup.h"

#include <memory>
#include <string>
#include <vector>

#include "client/vfs/metasystem/mds/chunk.h"
#include "client/vfs/metasystem/mds/chunk_memo.h"
#include "client/vfs/metasystem/mds/mds_client.h"
#include "common/helper.h"
#include "mds/common/runnable.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

DEFINE_uint32(vfs_meta_open_threshold_count, 8,
              "Threshold of open files to trigger warmup.");
DEFINE_validator(vfs_meta_open_threshold_count, brpc::PassValidate);

DEFINE_uint32(vfs_meta_warmup_readdir_interval_s, 8,
              "Interval seconds to trigger warmup readdir.");
DEFINE_validator(vfs_meta_warmup_readdir_interval_s, brpc::PassValidate);

// watch the open subfile read window count change, if the count is greater
// than the threshold, trigger warmup readdir and small file data and chunk.
class WarmupDirAccessStatsWatcher : public AccessStatsWatcher {
 public:
  WarmupDirAccessStatsWatcher(WarmupProcessor& warmup_processor)
      : warmup_processor_(warmup_processor) {}
  ~WarmupDirAccessStatsWatcher() override = default;

  void OnWindowCountChanged(DirAccessEvent event, Ino ino,
                            uint64_t count) override {
    if (event != DirAccessEvent::kOpenSubfileRead) return;
    if (!FLAGS_vfs_meta_warmup_small_file_enable) return;
    if (count < FLAGS_vfs_meta_open_threshold_count) return;

    // check dentry cache
    auto& dentry_cache = warmup_processor_.GetDentryCache();
    auto& warmup_memo = warmup_processor_.GetWarmupMemo();

    // maybe some files already fetch by readdir, so directly warmup data and
    // chunk
    std::vector<Ino> child_inoes = dentry_cache.ListFile(ino);
    warmup_processor_.WarmupSmallFileDataAndChunk(ino, child_inoes);

    // fetch other files in the same directory
    warmup_processor_.ExecuteReadDir(ino);
  }

 private:
  WarmupProcessor& warmup_processor_;
};

class WarmupChunkTask final : public mds::TaskRunnable {
 public:
  WarmupChunkTask(uint32_t fs_id, Ino parent, const std::vector<Ino>& inos,
                  WarmupProcessor& warmup_processor)
      : fs_id_(fs_id),
        parent_(parent),
        inoes_(inos),
        warmup_processor_(warmup_processor) {}

  std::string Type() override { return "WARMUP_CHUNK"; }
  std::string Key() override { return std::to_string(inoes_.front()); }

  void Run() override {
    Status status = DoWarmup();
    if (!status.ok()) {
      LOG(ERROR) << fmt::format(
          "[meta.warmup.{}] warmup chunk fail, error({}).", parent_,
          status.ToString());
    }
  }

 private:
  Status DoWarmup() {
    auto& chunk_memo = warmup_processor_.GetChunkMemo();
    auto& mds_client = warmup_processor_.GetMDSClient();
    auto& read_chunk_cache = warmup_processor_.GetReadChunkCache();

    LOG_DEBUG << fmt::format(
        "[meta.warmup] do warmup chunk, parent({}) child_count({}).", parent_,
        inoes_.size());

    std::vector<MDSClient::ReadSliceInEntry> in_entries;
    in_entries.reserve(inoes_.size());
    for (const auto& ino : inoes_) {
      MDSClient::ReadSliceInEntry in_entry;
      in_entry.ino = ino;
      in_entry.index = 0;
      in_entry.version = chunk_memo.GetVersion(ino, in_entry.index);
      in_entries.push_back(in_entry);
    }

    auto ctx = std::make_shared<Context>("");
    std::vector<MDSClient::ReadSliceOutEntry> out_entries;
    Status status = mds_client.ReadSlice(ctx, in_entries, out_entries);
    if (!status.ok() && !status.IsNotFound()) return status;

    for (const auto& entry : out_entries) {
      read_chunk_cache.Put(entry.ino, entry.chunk);
    }

    return Status::OK();
  }

  const uint32_t fs_id_;
  const Ino parent_;
  const std::vector<Ino> inoes_;

  WarmupProcessor& warmup_processor_;
};

class ReadDirTask : public mds::TaskRunnable {
 public:
  ReadDirTask(uint32_t fs_id, Ino ino, WarmupProcessor& warmup_processor)
      : fs_id_(fs_id), ino_(ino), warmup_processor_(warmup_processor) {}

  std::string Type() override { return "READ_DIR"; }
  std::string Key() override { return std::to_string(ino_); }

  void Run() override {
    auto& warmup_memo = warmup_processor_.GetWarmupMemo();

    if (warmup_memo.IsRemembered(ino_)) {
      LOG_DEBUG << fmt::format(
          "[meta.warmup.{}] warmup readdir skipped cause by remembered.", ino_);
      return;
    }

    warmup_memo.Remember(ino_);

    Status status = DoReadDir();
    if (!status.ok()) {
      LOG(ERROR) << fmt::format("[meta.warmup.{}] read dir fail, error({}).",
                                ino_, status.ToString());
    }
  }

 private:
  Status DoReadDir() {
    auto& mds_client = warmup_processor_.GetMDSClient();
    auto& inode_cache = warmup_processor_.GetInodeCache();
    auto& dentry_cache = warmup_processor_.GetDentryCache();

    LOG_DEBUG << fmt::format("[meta.warmup.{}] readdir by warmup.", ino_);

    auto ctx = std::make_shared<Context>("");
    ctx->reason = "warmup";

    std::string last_name;
    do {
      std::vector<MDSClient::ReadDirEntry> dentries;
      Status status = mds_client.ReadDir(ctx, ino_, 0, last_name,
                                         FLAGS_vfs_meta_read_dir_batch_size,
                                         true, dentries);

      if (!status.ok()) return status;

      std::vector<Ino> child_inoes;
      child_inoes.reserve(dentries.size());

      // cache inode and dentry
      for (auto& dentry : dentries) {
        if (!IsFile(dentry.ino)) continue;
        if (!dingofs::Helper::IsSmallFile(dentry.attr_entry.length())) continue;

        inode_cache.Put(dentry.ino, dentry.attr_entry);
        dentry_cache.Put(ino_, dentry.name, dentry.ino);
        child_inoes.push_back(dentry.ino);
      }

      // warmup small file data and chunk
      warmup_processor_.WarmupSmallFileDataAndChunk(ino_, child_inoes);

      if (dentries.size() < FLAGS_vfs_meta_read_dir_batch_size) break;

      last_name = dentries.back().name;

    } while (true);

    return Status::OK();
  }

  const uint32_t fs_id_;
  const Ino ino_;

  WarmupProcessor& warmup_processor_;
};

void WarmupMemo::Remember(Ino ino) {
  shard_map_.withWLock(
      [&](Map& map) {
        auto [it, inserted] = map.try_emplace(ino, Value{utils::Timestamp()});
        if (!inserted) {
          it->second.last_time_s = utils::Timestamp();
        }
      },
      ino);
}

void WarmupMemo::Forget(Ino ino) {
  shard_map_.withWLock([&](Map& map) { map.erase(ino); }, ino);
}

bool WarmupMemo::IsRemembered(Ino ino) {
  uint64_t now = utils::Timestamp();
  bool remembered = false;
  shard_map_.withRLock(
      [&](const Map& map) {
        auto it = map.find(ino);
        if (it != map.end() &&
            now <= (it->second.last_time_s +
                    FLAGS_vfs_meta_warmup_readdir_interval_s)) {
          remembered = true;
        }
      },
      ino);

  return remembered;
}

void WarmupMemo::CleanExpired(uint64_t expire_s) {
  if (Size() < FLAGS_vfs_meta_clean_threshold_count) return;

  shard_map_.withWLock([&](Map& map) {
    for (auto it = map.begin(); it != map.end();) {
      if (it->second.last_time_s < expire_s) {
        auto temp = it++;
        map.erase(temp);

      } else {
        ++it;
      }
    }
  });
}

size_t WarmupMemo::Size() {
  size_t total_size = 0;
  shard_map_.withRLock([&](const Map& map) { total_size = map.size(); });
  return total_size;
}

size_t WarmupMemo::Bytes() { return Size() * (sizeof(Value) + sizeof(Ino)); }

bool WarmupProcessor::Init() {
  access_stats_map_.RegisterWatcher(
      std::make_unique<WarmupDirAccessStatsWatcher>(*this));
  return true;
}

void WarmupProcessor::ExecuteReadDir(Ino ino) {
  if (warmup_memo_.IsRemembered(ino)) return;

  auto task = std::make_shared<ReadDirTask>(fs_id_, ino, *this);

  if (!executor_.ExecuteByHash(ino, task)) {
    LOG(ERROR) << fmt::format(
        "[meta.warmup] submit warmup readdir task fail, ino({}).", ino);
  }
}

void WarmupProcessor::WarmupSmallFileData(Ino parent,
                                          const std::vector<Ino>& inos) {
  if (inos.empty()) return;
  if (warmup_manager_ == nullptr) return;

  LOG_DEBUG << fmt::format(
      "[meta.warmup] submit warmup data task, parent({}) child_count({}).",
      parent, inos.size());

  // warmup small file data
  Status status = warmup_manager_->SubmitTask(WarmupTaskContext(inos));
  if (!status.ok()) {
    LOG(ERROR) << fmt::format(
        "[meta.warmup] submit warmup task fail, inos({}) error({}).", inos,
        status.ToString());
  }
}

void WarmupProcessor::WarmupSmallFileChunk(Ino parent,
                                           const std::vector<Ino>& inos) {
  if (inos.empty()) return;

  LOG_DEBUG << fmt::format(
      "[meta.warmup] submit warmup chunk task, parent({}) child_count({}).",
      parent, inos.size());

  auto task = std::make_shared<WarmupChunkTask>(fs_id_, parent, inos, *this);

  if (!executor_.ExecuteByHash(inos.front(), task)) {
    LOG(ERROR) << fmt::format(
        "[meta.warmup] submit warmup chunk task fail, inos({}).", inos);
  }
}

void WarmupProcessor::WarmupSmallFileDataAndChunk(
    Ino parent, const std::vector<Ino>& inos) {
  std::vector<Ino> warmup_inoes;
  warmup_inoes.reserve(inos.size());
  for (auto ino : inos) {
    if (!warmup_memo_.IsRemembered(ino)) warmup_inoes.push_back(ino);
  }

  if (warmup_inoes.empty()) return;

  for (const auto& ino : warmup_inoes) warmup_memo_.Remember(ino);

  WarmupSmallFileData(parent, warmup_inoes);
  WarmupSmallFileChunk(parent, warmup_inoes);
}

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs