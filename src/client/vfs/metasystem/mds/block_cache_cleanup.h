
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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_MDS_BLOCK_CACHE_CLEANUP_H_
#define DINGOFS_SRC_CLIENT_VFS_META_MDS_BLOCK_CACHE_CLEANUP_H_

#include "client/vfs/blockstore/block_store.h"
#include "client/vfs/metasystem/mds/executor.h"
#include "client/vfs/metasystem/mds/mds_client.h"
#include "mds/common/runnable.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

class BlockCacheCleaner;

class BlockCacheCleanupTask final : public mds::TaskRunnable {
 public:
  BlockCacheCleanupTask(uint32_t fs_id, Ino ino, uint64_t length,
                        BlockCacheCleaner& block_cache_cleaner)
      : fs_id_(fs_id),
        ino_(ino),
        length_(length),
        block_cache_cleaner_(block_cache_cleaner) {}

  std::string Type() override { return "BLOCK_CACHE_CLEANUP"; }
  std::string Key() override { return std::to_string(ino_); }

  void Run() override;

 private:
  Status Clean();

  const uint32_t fs_id_;
  const Ino ino_;
  const uint64_t length_;

  BlockCacheCleaner& block_cache_cleaner_;
};

class BlockCacheCleaner {
 public:
  BlockCacheCleaner(uint32_t fs_id, uint64_t chunk_size, Executor& executor,
                    MDSClient& mds_client)
      : fs_id_(fs_id),
        chunk_size_(chunk_size),
        executor_(executor),
        mds_client_(mds_client) {}

  void Stop();

  void SetBlockStore(BlockStore* block_store) { block_store_ = block_store; }

  void Execute(Ino ino, uint64_t length);

 private:
  friend class BlockCacheCleanupTask;

  uint64_t GetChunkSize() const { return chunk_size_; }
  MDSClient& GetMdsClient() { return mds_client_; }
  BlockStore* GetBlockStore() { return block_store_; }
  bool IsStopped() const { return is_stopped_.load(); }

  const uint32_t fs_id_;
  const uint64_t chunk_size_;

  Executor& executor_;
  MDSClient& mds_client_;
  BlockStore* block_store_{nullptr};

  std::atomic<bool> is_stopped_{false};
};

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_MDS_BLOCK_CACHE_CLEANUP_H_
