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
#include <butil/time.h>

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

DEFINE_uint32(upload_stage_max_tries, 3,
              "maximum tries per round for uploading one stage block to "
              "storage, a failed round is re-enqueued on a slow cycle");
DEFINE_validator(upload_stage_max_tries, brpc::PassValidate);

DEFINE_uint32(upload_stage_retry_delay_s, 60,
              "delay in seconds before re-enqueueing the stage block whose "
              "upload failed");
DEFINE_validator(upload_stage_retry_delay_s, brpc::PassValidate);

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

  // Flip running_ then wake every parked retry bthread so they observe the
  // shutdown and exit without re-enqueueing.
  running_.store(false, std::memory_order_relaxed);
  {
    std::lock_guard<bthread::Mutex> lk(park_mutex_);
    park_cond_.notify_all();
  }

  // Join the worker before the joiner: the worker is the only spawner, so
  // no BackgroundJoin can race the joiner queue stop below.
  thread_.join();

  joiner_->Shutdown();
  LOG(INFO) << "BlockCacheUploader is down";
}

void BlockCacheUploader::EnterUploadQueue(const StageBlock& sblock) {
  if (!IsRunning()) {
    // e.g. a stage callback racing shutdown: the block is durable on disk
    // and will be re-enqueued by reload on next start.
    LOG(WARNING) << "Uploader is down, skip enqueueing " << sblock;
    return;
  }
  pending_queue_->Push(sblock);
}

void BlockCacheUploader::UploadWorker() {
  // Not a CHECK: a shutdown racing right behind Start() can legally flip
  // running_ before this worker thread gets scheduled.
  if (!IsRunning()) {
    return;
  }

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
  auto status = tracker_->Add(sblock.key.Filename());
  if (status.IsExist()) {
    // e.g. a watcher reload raced the slow retry cycle of the same block:
    // the inflight upload already owns it, a duplicate bthread would
    // corrupt the inflight accounting.
    LOG(WARNING) << "Skip duplicate upload for inflight block: " << sblock;
    return;
  }

  auto* self = GetSelfPtr();
  auto tid = iutil::RunInBthread([self, sblock]() {
    auto status = self->DoUpload(sblock);
    // Release the inflight slot before OnComplete: a failed upload waits for
    // the slow retry cycle there and must not pin the slot while waiting.
    self->tracker_->Remove(sblock.key.Filename());
    self->OnComplete(sblock, status);
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

  // error: re-enqueue on a slow cycle, the stage block is durable on local
  // disk so it retries indefinitely until success. The fast bounded retries
  // already happened inside StorageClient::Put.
  LOG(ERROR) << "Fail to upload " << sblock << ", status = "
             << status.ToString() << ", it will retry after "
             << FLAGS_upload_stage_retry_delay_s << " s";

  // Park on the shared condvar rather than sleep-polling: shutdown wakes
  // all parked bthreads at once instead of each polling every 100ms.
  const int64_t deadline_us =
      butil::gettimeofday_us() + (FLAGS_upload_stage_retry_delay_s * 1000000LL);
  {
    std::unique_lock<bthread::Mutex> lk(park_mutex_);
    while (IsRunning()) {
      int64_t now_us = butil::gettimeofday_us();
      if (now_us >= deadline_us) {
        break;
      }
      park_cond_.wait_for(lk, deadline_us - now_us);
    }
  }

  if (IsRunning()) {
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
  status = storage_client->Put(sblock.ctx, sblock.key, &block,
                               {.max_tries = FLAGS_upload_stage_max_tries});
  if (status.IsNotFound()) {
    // Storage 404 (e.g. missing bucket) is NOT the stage-file-deleted
    // NotFound which OnComplete treats as terminal; surface it as an IO
    // error so the block stays on the slow retry cycle.
    LOG(ERROR) << "Fail to put " << sblock
               << " to storage with 404, bucket missing or misconfigured?";
    return Status::IoError("storage put got 404");
  } else if (!status.ok()) {
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
