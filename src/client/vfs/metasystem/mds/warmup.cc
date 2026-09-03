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
    if (!warmup_processor_.IsEnableBlockCache()) return;
    if (count < FLAGS_vfs_meta_open_threshold_count) return;

    warmup_processor_.AsyncWarmupSmallFile(ino);
  }

 private:
  WarmupProcessor& warmup_processor_;
};

class WarmupTask final : public mds::TaskRunnable {
 public:
  WarmupTask(Ino ino, WarmupProcessor& warmup_processor)
      : ino_(ino), warmup_processor_(warmup_processor) {}

  std::string Type() override { return "WARMUP"; }
  std::string Key() override { return std::to_string(ino_); }

  void Run() override {
    // check dentry cache
    auto& dentry_cache = warmup_processor_.GetDentryCache();
    auto& warmup_memo = warmup_processor_.GetWarmupMemo();

    // maybe some files already fetch by readdir, so directly warmup data and
    // chunk
    std::vector<Ino> child_inoes = dentry_cache.ListFile(ino_);
    warmup_processor_.AsyncWarmupSmallFileDataAndChunk(ino_, child_inoes);

    // fetch other files in the same directory
    Status status = warmup_processor_.DoWarmupReadDir(ino_);
    if (!status.ok()) {
      LOG(ERROR) << fmt::format(
          "[meta.warmup.{}] warmup readdir fail, error({}).", ino_,
          status.ToString());
    }
  }

 private:
  const Ino ino_;

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
    Status status = warmup_processor_.DoWarmupSmallFileChunk(parent_, inoes_);
    if (!status.ok()) {
      LOG(ERROR) << fmt::format(
          "[meta.warmup.{}] warmup chunk fail, error({}).", parent_,
          status.ToString());
    }
  }

 private:
  const uint32_t fs_id_;
  const Ino parent_;
  const std::vector<Ino> inoes_;

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

bool WarmupMemo::CheckAndRemember(Ino ino, uint64_t now_s) {
  bool remembered = false;
  shard_map_.withWLock(
      [&](Map& map) {
        auto [it, inserted] = map.try_emplace(ino, Value{utils::Timestamp()});
        if (!inserted) {
          if (now_s <= (it->second.last_time_s +
                        FLAGS_vfs_meta_warmup_readdir_interval_s)) {
            remembered = true;
          } else {
            it->second.last_time_s = now_s;
          }
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

void WarmupProcessor::AsyncWarmupSmallFile(Ino parent) {
  if (warmup_manager_ == nullptr) return;

  auto task = std::make_shared<WarmupTask>(parent, *this);
  if (!executor_.ExecuteByHash(parent, task, false)) {
    LOG(ERROR) << fmt::format("[meta.warmup.{}] submit warmup task fail.",
                              parent);
  }
}

void WarmupProcessor::DoWarmupSmallFileData(Ino parent,
                                            const std::vector<Ino>& inos) {
  if (inos.empty()) return;
  if (warmup_manager_ == nullptr) return;

  LOG_DEBUG << fmt::format("[meta.warmup.{}] do warmup data, child_count({}).",
                           parent, inos.size());

  // warmup small file data
  Status status = warmup_manager_->SubmitTask(WarmupTaskContext(inos));
  if (!status.ok()) {
    LOG(ERROR) << fmt::format(
        "[meta.warmup.{}] submit warmup data task fail, inos({}) error({}).",
        parent, inos, status.ToString());
  }
}

Status WarmupProcessor::DoWarmupSmallFileChunk(Ino parent,
                                               const std::vector<Ino>& inos) {
  if (inos.empty()) return Status::OK();

  LOG_DEBUG << fmt::format("[meta.warmup.{}] do warmup chunk, child_count({}).",
                           parent, inos.size());

  std::vector<MDSClient::ReadSliceInEntry> in_entries;
  in_entries.reserve(inos.size());
  for (const auto& ino : inos) {
    MDSClient::ReadSliceInEntry in_entry;
    in_entry.ino = ino;
    in_entry.index = 0;
    in_entry.version = chunk_memo_.GetVersion(ino, in_entry.index);
    in_entries.push_back(in_entry);
  }

  auto ctx = std::make_shared<Context>("");
  std::vector<MDSClient::ReadSliceOutEntry> out_entries;
  Status status = mds_client_.ReadSlice(ctx, in_entries, out_entries);
  if (!status.ok() && !status.IsNotFound()) return status;

  for (const auto& entry : out_entries) {
    read_chunk_cache_.Put(entry.ino, entry.chunk);
  }

  return Status::OK();
}

void WarmupProcessor::AsyncWarmupSmallFileChunk(Ino parent,
                                                const std::vector<Ino>& inos) {
  if (inos.empty()) return;

  auto task = std::make_shared<WarmupChunkTask>(fs_id_, parent, inos, *this);

  if (!executor_.ExecuteLeastQueue(task)) {
    LOG(ERROR) << fmt::format(
        "[meta.warmup.{}] submit warmup chunk task fail, inos({}).", parent,
        inos);
  }
}

void WarmupProcessor::AsyncWarmupSmallFileDataAndChunk(
    Ino parent, const std::vector<Ino>& inos) {
  std::vector<Ino> warmup_inoes;
  warmup_inoes.reserve(inos.size());
  uint64_t now_s = utils::Timestamp();
  for (const auto& ino : inos) {
    if (!warmup_memo_.CheckAndRemember(ino, now_s)) warmup_inoes.push_back(ino);
  }

  if (warmup_inoes.empty()) return;

  DoWarmupSmallFileData(parent, warmup_inoes);
  AsyncWarmupSmallFileChunk(parent, warmup_inoes);
}

Status WarmupProcessor::DoWarmupReadDir(Ino parent) {
  if (warmup_memo_.IsRemembered(parent)) return Status::OK();
  warmup_memo_.Remember(parent);

  LOG_DEBUG << fmt::format("[meta.warmup.{}] do warmup readdir.", parent);

  auto ctx = std::make_shared<Context>("");
  ctx->reason = "warmup";

  std::string last_name;
  do {
    std::vector<MDSClient::ReadDirEntry> dentries;
    Status status =
        mds_client_.ReadDir(ctx, parent, 0, last_name,
                            FLAGS_vfs_meta_read_dir_batch_size, true, dentries);

    if (!status.ok()) return status;

    std::vector<Ino> child_inoes;
    child_inoes.reserve(dentries.size());

    // cache inode and dentry
    for (auto& dentry : dentries) {
      if (!IsFile(dentry.ino)) continue;
      if (!dingofs::Helper::IsSmallFile(dentry.attr_entry.length())) continue;

      inode_cache_.Put(dentry.ino, dentry.attr_entry);
      dentry_cache_.Put(parent, dentry.name, dentry.ino);
      child_inoes.push_back(dentry.ino);
    }

    // warmup small file data and chunk
    AsyncWarmupSmallFileDataAndChunk(parent, child_inoes);

    if (dentries.size() < FLAGS_vfs_meta_read_dir_batch_size) break;

    last_name = dentries.back().name;

  } while (true);

  return Status::OK();
}

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs