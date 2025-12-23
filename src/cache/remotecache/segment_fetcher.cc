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

#include "cache/remotecache/segment_fetcher.h"

#include <bthread/countdown_event.h>
#include <bthread/execution_queue.h>
#include <bthread/execution_queue_inl.h>
#include <bthread/mutex.h>
#include <butil/time.h>
#include <glog/logging.h>
#include <sys/types.h>

#include <atomic>
#include <cstdio>
#include <memory>

#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/cache_store.h"
#include "cache/common/const.h"
#include "cache/common/macro.h"
#include "cache/storage/storage.h"
#include "cache/utils/bthread.h"
#include "cache/utils/cache.h"
#include "cache/utils/context.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

Segment* BlockMap::Block::GetOrCreateSegment(int index) {
  Segment* segment = segments_[index].load(std::memory_order_consume);
  if (segment != nullptr) {
    return segment;
  }

  Segment* expected = nullptr;
  segment = new (std::nothrow) Segment(index);  // TODO(Wine93): new failed?
  if (segments_[index].compare_exchange_strong(expected, segment,
                                               std::memory_order_release,
                                               std::memory_order_consume)) {
    return segment;
  }

  delete segment;
  return expected;
}

BlockMap::BlockSPtr BlockMap::GetOrCreateBlock(const BlockKey& key) {
  uint64_t slice_id = key.id;
  uint64_t block_index = key.index;
  auto* m = &blocks_[block_index % kBlockNum];

  {
    bthread::RWLockRdGuard guard(rwlock_);
    auto* stat = m->seek(slice_id);
    if (stat != nullptr) {
      return *stat;
    }
  }

  bthread::RWLockWrGuard guard(rwlock_);
  auto* stat = m->seek(slice_id);
  if (stat != nullptr) {
    return *stat;
  }

  return *m->insert(slice_id, std::make_shared<Block>());
}

SegmentFetcher::SegmentFetcher(Cache* cache, RemoteCacheNodeSPtr remote_node)
    : cache_(cache),
      remote_node_(remote_node),
      joiner_(std::make_unique<BthreadJoiner>()) {}

void SegmentFetcher::Start() {
  bthread::ExecutionQueueOptions options;
  options.use_pthread = true;

  CHECK_EQ(0, bthread::execution_queue_start(&task_queue_id_, &options,
                                             HandleTasks, this));
  CHECK_EQ(0, bthread::execution_queue_start(&buffer_queue_id_, &options,
                                             HandleBuffer, this));

  CHECK(joiner_->Start().ok());
}

void SegmentFetcher::Shutdown() {
  CHECK_EQ(0, bthread::execution_queue_stop(task_queue_id_));
  CHECK_EQ(0, bthread::execution_queue_join(task_queue_id_));

  CHECK_EQ(0, bthread::execution_queue_stop(buffer_queue_id_));
  CHECK_EQ(0, bthread::execution_queue_join(buffer_queue_id_));
}

SegmentFetcher::Task* SegmentFetcher::GetTaskEntry(const BlockKey& block_key,
                                                   size_t block_length,
                                                   Segment* segment) {
  if (segment->SetFetching()) {  // FIXME
    segment->ResetEvent();
    auto* task = new Task();
    task->block_key = block_key;
    task->block_length = block_length;
    task->segment = segment;
    return task;
  }

  return nullptr;  // already fetching or cached
}

void SegmentFetcher::SubmitTasks(const std::vector<Task*>& tasks) {
  CHECK_EQ(0, bthread::execution_queue_execute(task_queue_id_, tasks));
}

int SegmentFetcher::HandleTasks(
    void* meta, bthread::TaskIterator<std::vector<Task*>>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  auto* self = static_cast<SegmentFetcher*>(meta);
  for (; iter; iter++) {
    for (auto* task : *iter) {
      self->HandleTask(task);
    }
  }
  return 0;
}

void SegmentFetcher::HandleTask(Task* task) {
  // TODO(Wine93): use bthread pool
  auto tid = RunInBthread([this, task]() { DoFetch(task); });
  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }
}

void SegmentFetcher::DoFetch(Task* task) {
  auto* segment = task->segment;
  off_t offset = segment->GetIndex() * kSegmentSize;
  size_t length = std::min(task->block_length - offset, kSegmentSize);
  auto* buffer = new IOBuffer();
  auto status = remote_node_->Range(NewContext(), task->block_key, offset,
                                    length, buffer, RangeOption());

  if (status.ok()) {
    OnSuccess(task, status, buffer);
  } else {
    OnFailure(task, status, buffer);
  }

  segment->WakeupWaiters();

  delete task;
}

void SegmentFetcher::OnSuccess(Task* task, Status /*status*/,
                               IOBuffer* buffer) {
  LOG(INFO) << "Success to fetch segment: key = " << task->block_key.Filename()
            << ", segment_index = " << task->segment->GetIndex()
            << ", length = " << buffer->Size();

  auto* segment = task->segment;
  auto* value = new CacheEntry{this, segment, buffer};
  auto* handle = cache_->Insert(
      SegmentCacheKey(task->block_key, segment->GetIndex()), value,
      buffer->Size(), [](const std::string_view& key, void* value) {
        HandleCacheEvict(key, value);
      });
  cache_->Release(handle);

  CHECK(task->segment->SetCached());
}

void SegmentFetcher::OnFailure(Task* task, Status status, IOBuffer* buffer) {
  LOG(WARNING) << "Fail to fetch segment: key = " << task->block_key.Filename()
               << ", segment_index = " << task->segment->GetIndex()
               << ", status = " << status.ToString();

  delete buffer;

  auto* segment = task->segment;
  CHECK(segment->SetIdle(Segment::State::kFetching));
}

void SegmentFetcher::HandleCacheEvict(const std::string_view& key,
                                      void* value) {
  butil::Timer timer;
  timer.start();

  auto* e = static_cast<CacheEntry*>(value);
  auto* self = e->self;
  CHECK(e->segment->SetIdle(Segment::State::kCached));
  self->DeferFreeBuffer(e->buffer);

  timer.stop();

  // FIXME: VLOG
  LOG(INFO) << "Evict segment from cache: key = " << key << ", cost "
            << timer.n_elapsed(0) << " ns";
}

void SegmentFetcher::DeferFreeBuffer(IOBuffer* buffer) {
  CHECK_EQ(0, bthread::execution_queue_execute(buffer_queue_id_, buffer));
}

int SegmentFetcher::HandleBuffer(void* /*meta*/,
                                 bthread::TaskIterator<IOBuffer*>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  for (; iter; iter++) {
    delete *iter;
  }
  return 0;
}

SegmentHandler::SegmentHandler(Cache* cache, Storage* storage, Segment* segment,
                               bool waiting)
    : cache_(cache), storage_(storage), segment_(segment), waiting_(waiting) {}

Status SegmentHandler::Handle(const BlockKey& key, off_t offset, size_t length,
                              IOBuffer* buffer) {
  if (waiting_) {
    segment_->WaitFetched();
  }

  auto index = segment_->GetIndex();
  off_t boff_l = offset;  // block offset (left bound)
  off_t boff_r = offset + length;
  off_t soff_l = index * kSegmentSize;  // segment offset (left bound)
  off_t soff_r = soff_l + kSegmentSize;
  off_t off_l = std::max(boff_l, soff_l);  // offset in current request
  off_t off_r = std::min(boff_r, soff_r);

  auto* handle = cache_->Lookup(SegmentCacheKey(key, index));
  if (handle != nullptr) {
    SCOPE_EXIT { cache_->Release(handle); };
    auto* sbuffer =
        static_cast<SegmentFetcher::CacheEntry*>(cache_->Value(handle))->buffer;
    sbuffer->AppendTo(buffer, off_r - off_l, off_l % kSegmentSize);
    return Status::OK();
  }

  IOBuffer piece;
  auto status =
      storage_->Download(NewContext(), key, off_l, off_r - off_l, &piece);
  if (status.ok()) {
    buffer->Append(&piece);
  }
  return status;
}

CacheRetriever::CacheRetriever(RemoteCacheNodeSPtr remote_node,
                               Storage* storage)
    : block_map_(std::make_unique<SharedBlockMap>()),
      cache_(NewLRUCache(1024 * kMiB)),
      fetcher_(std::make_unique<SegmentFetcher>(cache_, remote_node)),
      storage_(storage) {}

void CacheRetriever::Start() { fetcher_->Start(); }

void CacheRetriever::Shutdown() { fetcher_->Shutdown(); }

// NOTE: must gurantee takes less 50us per 128KB request
Status CacheRetriever::Range(const BlockKey& key, off_t offset, size_t length,
                             size_t block_length, IOBuffer* buffer) {
  CHECK_GT(block_length, 0);

  std::vector<SegmentHandler> handlers;
  std::vector<SegmentFetcher::Task*> to_fetch;
  const auto& block = block_map_->GetBlock(key);

  auto lindex = SegmentIndex(offset);
  auto rindex = SegmentIndex(offset + length - 1);
  auto tindex = SegmentIndex(block_length - 1);  // total index

  for (off_t index = lindex; index <= tindex; ++index) {
    auto* segment = block->GetSegment(index);
    auto st = segment->GetState();
    bool care = (index <= rindex);

    if (st.IsCached()) {
      if (care) {
        handlers.emplace_back(SegmentHandler(cache_, storage_, segment, false));
        // VLOG(3) << "Pick cache segment (" << index << ")";
      }
    } else if (st.IsFetching()) {
      if (care) {
        handlers.emplace_back(SegmentHandler(cache_, storage_, segment, true));
      }
    } else {
      auto* task = fetcher_->GetTaskEntry(key, block_length, segment);
      if (task != nullptr) {
        to_fetch.emplace_back(task);
      }

      if (care) {
        handlers.emplace_back(SegmentHandler(cache_, storage_, segment, true));
      }
    }
  }

  // Submit fetch tasks
  fetcher_->SubmitTasks(to_fetch);

  // Handler segment by sequence
  for (auto& handler : handlers) {
    auto status = handler.Handle(key, offset, length, buffer);
    if (!status.ok()) {
      return status;
    }
  }
  return Status::OK();
}

};  // namespace cache
};  // namespace dingofs
