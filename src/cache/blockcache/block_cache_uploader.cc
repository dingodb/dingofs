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
 * Created Date: 2024-09-25
 * Author: Jingli Chen (Wine93)
 */

#include "cache/blockcache/block_cache_uploader.h"

#include <brpc/reloadable_flags.h>
#include <bthread/bthread.h>
#include <bthread/mutex.h>

#include <atomic>
#include <memory>

#include "cache/blockcache/cache_store.h"
#include "cache/common/context.h"
#include "cache/common/macro.h"
#include "cache/iutil/bthread.h"
#include "cache/iutil/inflight_tracker.h"

namespace dingofs {
namespace cache {

DEFINE_uint32(
    upload_stage_max_inflights, 32,
    "maximum inflight requests for uploading stage blocks to storage");

// Allow you push one element and pop a bunch of elements at once.
template <typename T>
class Segments {
 public:
  using Segment = std::vector<T>;
  explicit Segments(size_t segment_size) : segment_size_(segment_size) {};

  void Push(T element) {
    if (segments_.empty() || segments_.back().size() == segment_size_) {
      segments_.emplace(Segment());
    }
    segments_.back().push_back(element);
    size_++;
  }

  Segment Pop() {
    if (segments_.empty()) {
      return Segment();
    }

    auto segment = segments_.front();
    segments_.pop();
    CHECK_GE(size_, segment.size());
    size_ -= segment.size();
    return segment;
  }

  size_t Size() { return size_; }

 private:
  size_t size_{0};
  const size_t segment_size_;
  std::queue<Segment> segments_;
};

// PendingQueue is a priority queue for uploading staging blocks
// which will upload writeback blocks first, then reload blocks.
class PendingQueue {
 public:
  PendingQueue() = default;

  void Push(const StageBlock& sblock) {
    std::unique_lock<bthread::Mutex> lk(mutex_);
    auto from = sblock.block_attr.from;
    auto iter = queues_.find(from);
    if (iter == queues_.end()) {
      iter = queues_.emplace(from, Segments<StageBlock>(kSegmentSize)).first;
    }

    auto& queue = iter->second;
    queue.Push(sblock);
    count_[from]++;
  }

  std::vector<StageBlock> Pop() {
    static std::vector<BlockAttr::BlockFrom> pop_prority{
        BlockAttr::kFromWriteback,
        BlockAttr::kFromReload,
        BlockAttr::kFromUnknown,
    };

    std::unique_lock<bthread::Mutex> lk(mutex_);
    for (const auto& from : pop_prority) {
      auto iter = queues_.find(from);
      if (iter != queues_.end() && iter->second.Size() != 0) {
        auto sblocks = iter->second.Pop();
        CHECK(count_[from] >= sblocks.size());
        count_[from] -= sblocks.size();
        return sblocks;
      }
    }
    return std::vector<StageBlock>();
  }

  size_t Size() {
    std::unique_lock<bthread::Mutex> lk(mutex_);
    size_t size = 0;
    for (auto& item : queues_) {
      size += item.second.Size();
    }
    return size;
  }

  void Stat(struct StageBlockStat* stat) {
    std::unique_lock<bthread::Mutex> lk(mutex_);
    auto num_from_writeback = count_[BlockAttr::kFromWriteback];
    auto num_from_reload = count_[BlockAttr::kFromReload];
    auto num_total = num_from_writeback + num_from_reload;
    *stat = StageBlockStat(num_total, num_from_writeback, num_from_reload);
  }

 private:
  static constexpr size_t kSegmentSize = 100;

  bthread::Mutex mutex_;
  std::unordered_map<BlockAttr::BlockFrom, Segments<StageBlock>> queues_;
  std::unordered_map<BlockAttr::BlockFrom, uint64_t> count_;
};

BlockCacheUploader::BlockCacheUploader(
    CacheStoreSPtr store, StorageClientPoolSPtr storage_client_pool)
    : running_(false),
      store_(store),
      storage_client_pool_(storage_client_pool),
      pending_queue_(std::make_unique<PendingQueue>()),
      tracker_(std::make_unique<iutil::InflightTracker>(
          FLAGS_upload_stage_max_inflights)),
      joiner_(std::make_unique<iutil::BthreadJoiner>()) {}

BlockCacheUploader::~BlockCacheUploader() { Shutdown(); }

void BlockCacheUploader::Start() {
  if (running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "BlockCacheUploader is already started";
    return;
  }

  LOG(INFO) << "BlockCacheUploader is starting...";

  joiner_->Start();

  running_.store(true, std::memory_order_relaxed);
  thread_ = std::thread(&BlockCacheUploader::UploadWorker, this);
  LOG(INFO) << "BlockCacheUploader is up.";
}

void BlockCacheUploader::Shutdown() {
  if (!running_.load(std::memory_order_relaxed)) {
    LOG(INFO) << "BlockCacheUploader is already shutdown";
    return;
  }

  LOG(INFO) << "BlockCacheUploader is shutting down...";

  joiner_->Shutdown();

  running_.store(false, std::memory_order_relaxed);
  thread_.join();
  LOG(INFO) << "BlockCacheUploader is down";
}

void BlockCacheUploader::EnterUploadQueue(const StageBlock& sblock) {
  DCHECK_RUNNING("BlockCacheUploader");
  pending_queue_->Push(sblock);
}

void BlockCacheUploader::UploadWorker() {
  CHECK_RUNNING("BlockCacheUploader");

  WaitStoreReady();

  while (IsRunning()) {
    auto sblocks = pending_queue_->Pop();
    if (sblocks.empty()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      continue;
    }

    for (const auto& sblock : sblocks) {
      AsyncUpload(sblock);
    }
  }
}

void BlockCacheUploader::AsyncUpload(const StageBlock& sblock) {
  tracker_->Add(sblock.key.Filename());

  auto* self = GetSelfPtr();
  auto tid = iutil::RunInBthread([self, sblock]() {
    auto status = self->DoUpload(sblock);
    self->OnComplete(sblock, status);
    self->tracker_->Remove(sblock.key.Filename());
  });

  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }
}

void BlockCacheUploader::OnComplete(const StageBlock& sblock, Status status) {
  auto key = sblock.key;
  if (status.ok() || status.IsNotFound()) {
    return;
  } else if (status.IsCacheDown()) {
    LOG(ERROR) << "Fail to upload " << sblock
               << " because the cache is down, it will "
                  "re-upload after cache restart if the block still exists";
    return;
  }

  // error
  static const int sleep_ms = 100;
  LOG(ERROR) << "Fail to upload " << sblock << ", it will retry after "
             << sleep_ms << " ms";

  if (IsRunning()) {
    bthread_usleep(sleep_ms * 1000);
    EnterUploadQueue(sblock);  // retry
  }
}

Status BlockCacheUploader::DoUpload(const StageBlock& sblock) {
  IOBuffer buffer;
  auto status = store_->Load(sblock.ctx, sblock.key, 0, sblock.length, &buffer);
  if (status.IsNotFound()) {
    LOG(ERROR) << "Fail to upload " << sblock
               << " which already deleted, abort upload";
    return status;
  } else if (!status.ok()) {
    LOG(ERROR) << "Fail to upload " << sblock;
    return status;
  }

  StorageClient* storage_client;
  status =
      storage_client_pool_->GetStorageClient(sblock.key.fs_id, &storage_client);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to get storage client";
    return status;
  }

  Block block(std::move(buffer));
  status = storage_client->Put(sblock.ctx, sblock.key, &block);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to put " << sblock << " to storage";
    return status;
  }

  status = store_->RemoveStage(sblock.ctx, sblock.key,
                               {.block_attr = sblock.block_attr});
  if (!status.ok()) {
    LOG(WARNING) << "Fail to remove stage block, key=" << sblock.key.Filename();
    status = Status::OK();  // ignore removestage error
  }
  return status;
}

std::ostream& operator<<(std::ostream& os, const StageBlock& sblock) {
  os << "StageBlock{key=" << sblock.key.Filename()
     << " length=" << sblock.length << " attr=" << sblock.block_attr << "}";
  return os;
}

}  // namespace cache
}  // namespace dingofs
