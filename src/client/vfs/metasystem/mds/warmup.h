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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_MDS_WARMUP_H_
#define DINGOFS_SRC_CLIENT_VFS_META_MDS_WARMUP_H_

#include <vector>

#include "absl/container/flat_hash_map.h"
#include "client/vfs/components/warmup_manager.h"
#include "client/vfs/metasystem/mds/chunk.h"
#include "client/vfs/metasystem/mds/chunk_memo.h"
#include "client/vfs/metasystem/mds/dentry_cache.h"
#include "client/vfs/metasystem/mds/executor.h"
#include "client/vfs/metasystem/mds/inode_cache.h"
#include "client/vfs/metasystem/mds/mds_client.h"
#include "client/vfs/metasystem/mds/statistics.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

class WarmupProcessor;

// remember already warmup dir and file recently, avoid repeatedly warmup
class WarmupMemo {
 public:
  void Remember(Ino ino);
  void Forget(Ino ino);

  bool IsRemembered(Ino ino);
  bool CheckAndRemember(Ino ino, uint64_t now_s);

  void RememberTrigger(Ino ino);
  bool ShouldTrigger(Ino ino);

  void CleanExpired(uint64_t expire_s);

  size_t Size();
  size_t Bytes();

 private:
  struct Value {
    uint64_t last_time_s{0};
    uint64_t last_trigger_time_ms{0};
  };
  // ino -> last warmup timestamp
  using Map = absl::flat_hash_map<Ino, Value>;

  constexpr static size_t kShardNum = 128;
  utils::Shards<Map, kShardNum> shard_map_;
};

// warmup small files data and chunk and dentry
class WarmupProcessor {
 public:
  WarmupProcessor(uint32_t fs_id, Executor& executor, ChunkMemo& chunk_memo,
                  MDSClient& mds_client, InodeCache& inode_cache,
                  DentryCache& dentry_cache, ReadChunkCache& read_chunk_cache,
                  AccessStatsMap& access_stats_map)
      : fs_id_(fs_id),
        executor_(executor),
        chunk_memo_(chunk_memo),
        mds_client_(mds_client),
        inode_cache_(inode_cache),
        dentry_cache_(dentry_cache),
        read_chunk_cache_(read_chunk_cache),
        access_stats_map_(access_stats_map) {}
  ~WarmupProcessor() = default;

  bool Init();

  using WarmupDataManager = WarmupManager;
  void SetWarmupManager(WarmupDataManager* warmup_manager) {
    warmup_manager_ = warmup_manager;
  }

  void SetEnableBlockCache(bool enable) { enable_block_cache_ = enable; }
  bool IsEnableBlockCache() const { return enable_block_cache_; }

  void CleanExpired(uint64_t expire_s) { warmup_memo_.CleanExpired(expire_s); }

 private:
  friend class WarmupTask;
  friend class WarmupChunkTask;
  friend class WarmupDirAccessStatsWatcher;

  DentryCache& GetDentryCache() { return dentry_cache_; }
  WarmupMemo& GetWarmupMemo() { return warmup_memo_; }

  void AsyncWarmupSmallFile(Ino parent);

  void DoWarmupSmallFileData(Ino parent, const std::vector<Ino>& inos);

  Status DoWarmupSmallFileChunk(Ino parent, const std::vector<Ino>& inos);
  void AsyncWarmupSmallFileChunk(Ino parent, const std::vector<Ino>& inos);

  void AsyncWarmupSmallFileDataAndChunk(Ino parent,
                                        const std::vector<Ino>& inos);

  Status DoWarmupReadDir(Ino parent);

  const uint32_t fs_id_{0};

  bool enable_block_cache_{false};

  Executor& executor_;
  ChunkMemo& chunk_memo_;
  MDSClient& mds_client_;
  InodeCache& inode_cache_;
  DentryCache& dentry_cache_;
  ReadChunkCache& read_chunk_cache_;
  AccessStatsMap& access_stats_map_;

  WarmupDataManager* warmup_manager_{nullptr};

  WarmupMemo warmup_memo_;
};

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_MDS_WARMUP_H_