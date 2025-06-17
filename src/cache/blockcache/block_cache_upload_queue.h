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

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_BLOCK_CACHE_UPLOAD_QUEUE_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_BLOCK_CACHE_UPLOAD_QUEUE_H_

#include <cstdint>
#include <memory>

#include "cache/blockcache/cache_store.h"
#include "cache/common/type.h"
#include "cache/utils/context.h"
#include "cache/utils/segments.h"

namespace dingofs {
namespace cache {

struct StagingBlock {
  StagingBlock() = delete;

  StagingBlock(ContextSPtr ctx, const BlockKey& key, size_t length,
               BlockContext block_ctx)
      : ctx(ctx), key(key), length(length), block_ctx(block_ctx) {}

  ContextSPtr ctx;
  BlockKey key;
  size_t length;
  BlockContext block_ctx;
};

struct BlocksStat {
  BlocksStat() = default;

  BlocksStat(uint64_t num_total, uint64_t num_from_writeback,
             uint64_t num_from_reload)
      : num_total(num_total),
        num_from_writeback(num_from_writeback),
        num_from_reload(num_from_reload) {}

  uint64_t num_total;
  uint64_t num_from_writeback;
  uint64_t num_from_reload;
};

// PendingQueue is a priority queue for uploading staging blocks
// which will upload writeback blocks first, then reload blocks.
class PendingQueue {
 public:
  PendingQueue() = default;

  void Push(const StagingBlock& staging_block);

  std::vector<StagingBlock> Pop();

  size_t Size();

  void Stat(struct BlocksStat* stat);

 private:
  static constexpr size_t kSegmentSize = 100;

  BthreadMutex mutex_;
  std::unordered_map<BlockContext::BlockFrom, Segments<StagingBlock>> queues_;
  std::unordered_map<BlockContext::BlockFrom, uint64_t> count_;
};

using PendingQueueUPtr = std::unique_ptr<PendingQueue>;
using PendingQueueSPtr = std::shared_ptr<PendingQueue>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_BLOCK_CACHE_UPLOAD_QUEUE_H_
