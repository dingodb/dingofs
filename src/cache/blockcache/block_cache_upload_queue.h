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

#include <condition_variable>
#include <unordered_map>

#include "cache/blockcache/cache_store.h"
#include "cache/utils/segments.h"

namespace dingofs {
namespace cache {
namespace blockcache {

using dingofs::cache::utils::Segments;

struct StageBlock {
  StageBlock() : seq_num(0) {}

  StageBlock(const BlockKey& key, const std::string& stage_path,
             BlockContext ctx)
      : key(key), stage_path(stage_path), ctx(ctx) {
    static std::atomic<uint64_t> g_seq_num(1);
    seq_num = g_seq_num.fetch_add(1, std::memory_order_relaxed);
  }

  bool operator<(const StageBlock& other) const {
    static std::unordered_map<BlockFrom, uint8_t> priority{
        {BlockFrom::kCtoFlush, 0},
        {BlockFrom::kNoctoFlush, 1},
        {BlockFrom::kReload, 2},
        {BlockFrom::kUnknown, 3},
    };

    if (ctx.from == other.ctx.from) {
      return seq_num > other.seq_num;
    }
    return priority[ctx.from] > priority[other.ctx.from];
  }

  bool Valid() const { return seq_num > 0; }

  uint64_t seq_num;
  BlockKey key;
  std::string stage_path;
  BlockContext ctx;
};

struct StatBlocks {
  StatBlocks() = default;

  StatBlocks(uint64_t num_total, uint64_t num_from_cto, uint64_t num_from_nocto,
             uint64_t num_from_reload)
      : num_total(num_total),
        num_from_cto(num_from_cto),
        num_from_nocto(num_from_nocto),
        num_from_reload(num_from_reload) {}

  uint64_t num_total;
  uint64_t num_from_cto;
  uint64_t num_from_nocto;
  uint64_t num_from_reload;
};

class PendingQueue {
 public:
  PendingQueue() = default;

  void Push(const StageBlock& stage_block);

  std::vector<StageBlock> Pop(bool peek = false);

  size_t Size();

  void Stat(struct StatBlocks* stat);

 private:
  std::mutex mutex_;
  std::unordered_map<BlockFrom, Segments<StageBlock>> queues_;
  std::unordered_map<BlockFrom, uint64_t> count_;
  static constexpr uint64_t kSegmentSize = 100;
};

class UploadingQueue {
 public:
  explicit UploadingQueue(size_t capacity);

  void Start();

  void Stop();

  void Push(const StageBlock& stage_block);

  StageBlock Pop();

  size_t Size();

  size_t Capacity() const;

  void Stat(struct StatBlocks* stat);

 private:
  std::mutex mutex_;
  std::atomic<bool> running_;
  size_t capacity_;
  std::condition_variable not_empty_;
  std::condition_variable not_full_;
  std::priority_queue<StageBlock> queue_;
  std::unordered_map<BlockFrom, uint64_t> count_;
};

}  // namespace blockcache
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_BLOCK_CACHE_UPLOAD_QUEUE_H_
