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

#include "client/vfs_legacy/in_time_warmup_manager.h"

#include <bthread/execution_queue.h>

#include <cstdint>
#include <ctime>
#include <memory>
#include <mutex>
#include <utility>

#include "client/vfs_legacy/inode_wrapper.h"
#include "client/vfs_legacy/service/flat_file.h"
#include "client/vfs_legacy/service/flat_file_util.h"

namespace dingofs {
namespace client {

int IntimeWarmUpManager::Consume(void* meta,
                                 bthread::TaskIterator<InodeWarmUpTask>& iter) {
  if (iter.is_queue_stopped()) {
    LOG(INFO) << "IntimeWarmUpManager Execution queue stopped";
    return 0;
  }

  auto* inode_prefetch_manager = reinterpret_cast<IntimeWarmUpManager*>(meta);

  for (; iter; iter++) {
    InodeWarmUpTask& task = *iter;
    inode_prefetch_manager->Prefetch(task.inode_wrapper);
  }
  return 0;
}

void IntimeWarmUpManager::Start() {
  if (!running_.exchange(true)) {
    bthread::ExecutionQueueOptions queue_options;
    queue_options.use_pthread = true;
    if (bthread::execution_queue_start(&queue_id_, &queue_options, Consume,
                                       this) != 0) {
      LOG(FATAL) << "Failed to start execution queue";
    }
    LOG(INFO) << "IntimeWarmUpManager started "
              << ", chunk_size: " << chunk_size_
              << ", block_size: " << block_size_;
  }
}

void IntimeWarmUpManager::Stop() {
  if (running_.exchange(false)) {
    bthread::execution_queue_stop(queue_id_);
    bthread::execution_queue_join(queue_id_);
    if (bthread::execution_queue_join(queue_id_) != 0) {
      LOG(ERROR) << "Failed to join execution queue";
    } else {
      LOG(INFO) << "IntimeWarmUpManager stopped";
    }
  }
}

void IntimeWarmUpManager::Submit(std::shared_ptr<InodeWrapper> inode_wrapper) {
  CHECK_EQ(0, bthread::execution_queue_execute(queue_id_,
                                               InodeWarmUpTask{inode_wrapper}));
}

void IntimeWarmUpManager::Prefetch(
    std::shared_ptr<InodeWrapper> inode_wrapper) {
  uint64_t inode_id = inode_wrapper->GetInodeId();
  uint64_t file_len = inode_wrapper->GetLength();
  uint64_t fs_id = inode_wrapper->GetFsId();
  std::vector<BlockObj> block_obj;

  timespec m_time = inode_wrapper->GetMtime();

  {
    std::lock_guard<std::mutex> lg(mutex_);

    auto iter = inode_prefetch_info_.find(inode_id);
    if (iter != inode_prefetch_info_.end()) {
      auto old_m_time = iter->second.mtime;
      if (old_m_time.tv_sec != m_time.tv_sec ||
          old_m_time.tv_nsec != m_time.tv_nsec) {
        FlatFile flat_file =
            InodeWrapperToFlatFile(inode_wrapper, chunk_size_, block_size_);
        block_obj = flat_file.GetBlockObj(0, file_len);

        VLOG(1) << "try re-prefetch inodeId=" << inode_id
                << " file_len: " << file_len
                << " block_obj size: " << block_obj.size()
                << " new mtime: " << m_time.tv_sec << "." << m_time.tv_nsec
                << " old mtime: " << old_m_time.tv_sec << "."
                << old_m_time.tv_nsec
                << " old file_len: " << iter->second.file_len;

        iter->second.mtime = m_time;
        iter->second.file_len = file_len;
      } else {
        VLOG(6) << "Ignore prefetch inodeId=" << inode_id
                << " file_len: " << file_len
                << " old file_len: " << iter->second.file_len
                << " new mtime: " << m_time.tv_sec << "." << m_time.tv_nsec
                << " old mtime: " << iter->second.mtime.tv_sec << "."
                << iter->second.mtime.tv_nsec;
        return;
      }
    } else {
      FlatFile flat_file =
          InodeWrapperToFlatFile(inode_wrapper, chunk_size_, block_size_);
      block_obj = flat_file.GetBlockObj(0, file_len);

      inode_prefetch_info_.emplace(std::make_pair(
          inode_id, InodePrefetchInfo{inode_id, m_time, file_len}));

      VLOG(1) << "try prefetch inodeId=" << inode_id
              << " file_len: " << file_len
              << " block_obj size: " << block_obj.size()
              << " mtime: " << m_time.tv_sec << "." << m_time.tv_nsec;
    }
  }

  for (auto& obj : block_obj) {
    cache::BlockKey key(fs_id, inode_id, obj.chunk_id, obj.block_index,
                        obj.version);
    VLOG(3) << "try to prefetch inodeId=" << inode_id
            << " block: " << key.StoreKey() << " len: " << obj.obj_len;
    block_cache_->AsyncPrefetch(key, obj.obj_len, [key](Status status) {
      if (!status.ok()) {
        LOG(WARNING) << "Failed to prefetch block (key=" << key.Filename()
                     << "): " << status.ToString();
      }
    });
  }
}

}  // namespace client
}  // namespace dingofs