/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2025-11-27
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_REMOTECACHE_SEGMENT_FETCHER_H_
#define DINGOFS_SRC_CACHE_REMOTECACHE_SEGMENT_FETCHER_H_

#include <bthread/butex.h>
#include <bthread/condition_variable.h>
#include <bthread/countdown_event.h>
#include <bthread/execution_queue.h>
#include <bthread/execution_queue_inl.h>
#include <bthread/mutex.h>
#include <bthread/rwlock.h>
#include <butil/containers/flat_map.h>
#include <opentelemetry/nostd/internal/absl/meta/type_traits.h>
#include <sys/types.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "cache/blockcache/cache_store.h"
#include "cache/remotecache/remote_cache_node.h"
#include "cache/storage/storage.h"
#include "cache/utils/bthread.h"
#include "cache/utils/cache.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {

namespace cache {

inline std::string SegmentCacheKey(const BlockKey& key, int segment_index) {
  return key.Filename() + ":" + std::to_string(segment_index);
}

// const constexpr size_t kSegmentSize = 128 * 1024;  // 128KB
const constexpr size_t kSegmentSize = 1024 * 1024;  // 128KB

class Segment {
 public:
  struct State {
    enum Code : uint8_t {
      kIdle = 0,
      kFetching = 1,
      kCached = 2,
    };

    State(int code) : code(code) {}
    State Idle() const { return State(kIdle); }
    State Fetching() const { return State(kFetching); }
    State Cached() const { return State(kCached); }

    bool IsIdle() const { return code == kIdle; }
    bool IsFetching() const { return code == kFetching; }
    bool IsCached() const { return code == kCached; }

    uint8_t code;
  };

  explicit Segment(int index) : index_(index) {}

  int GetIndex() const { return index_; }
  State GetState() { return State(state_.load(std::memory_order_relaxed)); }

  bool SetIdle(State from) { return SetState(from, State::kIdle); }
  bool SetFetching() { return SetState(State::kIdle, State::kFetching); }
  bool SetCached() { return SetState(State::kFetching, State::kCached); }

  void ResetEvent() { event_.reset(); }
  void WaitFetched() { event_.wait(); }
  void WakeupWaiters() { event_.signal(); }

 private:
  bool SetState(State from, State to) {
    uint8_t expected = from.code;
    return state_.compare_exchange_strong(expected, to.code,
                                          std::memory_order_release,
                                          std::memory_order_relaxed);
  }

  int index_;
  std::atomic<uint8_t> state_{State::kIdle};
  bthread::CountdownEvent event_{0};
};

// block will be sliced into multiple segments, each segment is 128KB
class BlockMap {
 public:
  class Block {
   public:
    Segment* GetSegment(int index) { return GetOrCreateSegment(index); }

   private:
    static constexpr size_t kSegmentNum = 32;  // 4MB/128KB

    Segment* GetOrCreateSegment(int index);

    // TODO(wine93): free block and segment
    std::atomic<Segment*> segments_[kSegmentNum]{};
  };

  using BlockSPtr = std::shared_ptr<Block>;

  BlockMap() = default;
  virtual ~BlockMap() = default;

  virtual BlockSPtr GetBlock(const BlockKey& key) {
    return GetOrCreateBlock(key);
  }

 private:
  static constexpr size_t kBlockNum = 16;  // 64MB/4MB

  BlockSPtr GetOrCreateBlock(const BlockKey& key);

  bthread::RWLock rwlock_;
  butil::FlatMap<uint64_t, BlockSPtr> blocks_[kBlockNum];  // key: slice_id
};

using BlockMapUPtr = std::unique_ptr<BlockMap>;

class SharedBlockMap : public BlockMap {
 public:
  SharedBlockMap() = default;

  BlockSPtr GetBlock(const BlockKey& key) override {
    return shard_[key.id % kShardNum].GetBlock(key);
  }

 private:
  static constexpr size_t kShardNum = 16;

  BlockMap shard_[kShardNum];  // hash by slice id
};

class SegmentFetcher {
 public:
  struct Task {
    BlockKey block_key;
    size_t block_length;
    Segment* segment;
  };

  struct CacheEntry {
    SegmentFetcher* self;
    Segment* segment;
    IOBuffer* buffer;
  };

  SegmentFetcher(Cache* cache, RemoteCacheNodeSPtr remote_node);

  void Start();
  void Shutdown();

  Task* GetTaskEntry(const BlockKey& block_key, size_t block_length,
                     Segment* segment);
  void SubmitTasks(const std::vector<Task*>& tasks);

 private:
  static int HandleTasks(void* meta,
                         bthread::TaskIterator<std::vector<Task*>>& iter);
  void HandleTask(Task* task);
  void DoFetch(Task* task);
  void OnSuccess(Task* task, Status status, IOBuffer* buffer);
  void OnFailure(Task* task, Status status, IOBuffer* buffer);

  static void HandleCacheEvict(const std::string_view& key, void* value);
  void DeferFreeBuffer(IOBuffer* buffer);
  static int HandleBuffer(void* meta, bthread::TaskIterator<IOBuffer*>& iter);

  Cache* cache_;
  RemoteCacheNodeSPtr remote_node_;
  BthreadJoinerUPtr joiner_;
  bthread::ExecutionQueueId<std::vector<Task*>> task_queue_id_;
  bthread::ExecutionQueueId<IOBuffer*> buffer_queue_id_;
};

using SegmentFetcherUPtr = std::unique_ptr<SegmentFetcher>;

class SegmentHandler {
 public:
  SegmentHandler(Cache* cache, Storage* storage, Segment* segment,
                 bool waiting);

  Status Handle(const BlockKey& key, off_t offset, size_t length,
                IOBuffer* buffer);

 private:
  Cache* cache_;
  Storage* storage_;
  Segment* segment_;
  bool waiting_;
};

// It will trigger prefetch
class CacheRetriever {
 public:
  CacheRetriever(RemoteCacheNodeSPtr remote_node, Storage* storage);

  void Start();
  void Shutdown();

  Status Range(const BlockKey& key, off_t offset, size_t length,
               size_t block_length, IOBuffer* buffer);

 private:
  off_t SegmentIndex(off_t offset) { return offset / kSegmentSize; }

  BlockMapUPtr block_map_;
  Cache* cache_;
  SegmentFetcherUPtr fetcher_;
  Storage* storage_;
};

using CacheRetrieverUPtr = std::unique_ptr<CacheRetriever>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_SEGMENT_FETCHER_H_
