// Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_SRC_CLIENT_VFS_OLD_IN_TIME_WARMUP_MANAGER_H
#define DINGOFS_SRC_CLIENT_VFS_OLD_IN_TIME_WARMUP_MANAGER_H

#include <bthread/execution_queue.h>
#include <bthread/execution_queue_inl.h>

#include <cstdint>
#include <ctime>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "cache/blockcache/block_cache.h"
#include "client/vfs_old/inode_wrapper.h"

namespace dingofs {
namespace client {

class IntimeWarmUpManager {
 public:
  IntimeWarmUpManager(std::shared_ptr<cache::BlockCache> block_cache,
                      uint64_t chunk_size, uint64_t block_size)
      : block_cache_(std::move(block_cache)),
        chunk_size_(chunk_size),
        block_size_(block_size) {}

  ~IntimeWarmUpManager() = default;

  void Start();

  void Stop();

  void Submit(std::shared_ptr<InodeWrapper> inode_wrapper);

 private:
  struct InodePrefetchInfo {
    uint64_t inode_id;
    timespec mtime;
    uint64_t file_len;
  };

  struct InodeWarmUpTask {
    std::shared_ptr<InodeWrapper> inode_wrapper;
  };
  static int Consume(void* meta, bthread::TaskIterator<InodeWarmUpTask>& iter);

  void Prefetch(std::shared_ptr<InodeWrapper> inode_wrapper);

  cache::BlockCacheSPtr block_cache_;
  uint64_t chunk_size_{0};
  uint64_t block_size_{0};

  std::atomic<bool> running_{false};
  bthread::ExecutionQueueId<InodeWarmUpTask> queue_id_;

  std::mutex mutex_;
  std::unordered_map<uint64_t, InodePrefetchInfo> inode_prefetch_info_;
};

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_OLD_IN_TIME_WARMUP_MANAGER_H