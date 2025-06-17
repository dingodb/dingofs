/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Project: DingoFS
 * Created Date: 2024-09-25
 * Author: Jingli Chen (Wine93)
 */

#include "cache/blockcache/block_cache_upload_queue.h"

namespace dingofs {
namespace cache {

void PendingQueue::Push(const StagingBlock& staging_block) {
  std::unique_lock<BthreadMutex> lk(mutex_);
  auto from = staging_block.block_ctx.from;
  auto iter = queues_.find(from);
  if (iter == queues_.end()) {
    iter = queues_.emplace(from, Segments<StagingBlock>(kSegmentSize)).first;
  }

  auto& queue = iter->second;
  queue.Push(staging_block);
  count_[from]++;
}

std::vector<StagingBlock> PendingQueue::Pop() {
  static std::vector<BlockContext::BlockFrom> pop_prority{
      BlockContext::kFromWriteback,
      BlockContext::kFromReload,
      BlockContext::kFromUnknown,
  };

  std::unique_lock<BthreadMutex> lk(mutex_);
  for (const auto& from : pop_prority) {
    auto iter = queues_.find(from);
    if (iter != queues_.end() && iter->second.Size() != 0) {
      auto staging_blocks = iter->second.Pop();
      CHECK(count_[from] >= staging_blocks.size());
      count_[from] -= staging_blocks.size();
      return staging_blocks;
    }
  }
  return std::vector<StagingBlock>();
}

size_t PendingQueue::Size() {
  std::unique_lock<BthreadMutex> lk(mutex_);
  size_t size = 0;
  for (auto& item : queues_) {
    size += item.second.Size();
  }
  return size;
}

void PendingQueue::Stat(struct BlocksStat* stat) {
  std::unique_lock<BthreadMutex> lk(mutex_);
  auto num_from_writeback = count_[BlockContext::kFromWriteback];
  auto num_from_reload = count_[BlockContext::kFromReload];
  auto num_total = num_from_writeback + num_from_reload;
  *stat = BlocksStat(num_total, num_from_writeback, num_from_reload);
}

}  // namespace cache
}  // namespace dingofs
