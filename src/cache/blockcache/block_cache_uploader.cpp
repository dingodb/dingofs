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

#include "cache/blockcache/block_cache_uploader.h"

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "absl/cleanup/cleanup.h"
#include "cache/blockcache/block_cache_uploader_cmmon.h"
#include "cache/blockcache/cache_store.h"
#include "cache/common/errno.h"
#include "cache/common/local_filesystem.h"
#include "cache/common/log.h"
#include "cache/common/phase_timer.h"
#include "cache/common/segments.h"
#include "client/common/dynamic_config.h"

namespace dingofs {
namespace cache {
namespace blockcache {

USING_FLAG(drop_page_cache);

using cache::common::LocalFileSystem;
using cache::common::LogIt;
using cache::common::Phase;
using cache::common::PhaseTimer;

BlockCacheUploader::BlockCacheUploader(std::shared_ptr<S3Client> s3,
                                       std::shared_ptr<CacheStore> store,
                                       std::shared_ptr<Countdown> stage_count)
    : running_(false), s3_(s3), store_(store), stage_count_(stage_count) {
  scan_stage_thread_pool_ =
      std::make_unique<TaskThreadPool<>>("scan_stage_worker");
  upload_stage_thread_pool_ =
      std::make_unique<TaskThreadPool<>>("upload_stage_worker");
}

void BlockCacheUploader::Init(uint64_t upload_workers,
                              uint64_t upload_queue_size) {
  if (!running_.exchange(true)) {
    // pending and uploading queue
    pending_queue_ = std::make_shared<PendingQueue>();
    uploading_queue_ = std::make_shared<UploadingQueue>(upload_queue_size);

    // scan stage block worker
    CHECK(scan_stage_thread_pool_->Start(1) == 0);
    scan_stage_thread_pool_->Enqueue(&BlockCacheUploader::ScaningWorker, this);

    // upload stage block worker
    CHECK(upload_stage_thread_pool_->Start(upload_workers) == 0);
    for (uint64_t i = 0; i < upload_workers; i++) {
      upload_stage_thread_pool_->Enqueue(&BlockCacheUploader::UploadingWorker,
                                         this);
    }
  }
}

void BlockCacheUploader::Shutdown() {
  if (running_.exchange(false)) {
    scan_stage_thread_pool_->Stop();
    upload_stage_thread_pool_->Stop();
  }
}

void BlockCacheUploader::AddStageBlock(const BlockKey& key,
                                       const std::string& stage_path,
                                       BlockContext ctx) {
  static std::atomic<uint64_t> g_seq_num(0);
  auto seq_num = g_seq_num.fetch_add(1, std::memory_order_relaxed);
  StageBlock stage_block(seq_num, key, stage_path, ctx);
  Staging(stage_block);
  pending_queue_->Push(stage_block);
}

// Reserve space for stage blocks which from |CTO_FLUSH|
bool BlockCacheUploader::CanUpload(const std::vector<StageBlock>& blocks) {
  if (blocks.empty()) {
    return false;
  }
  auto from = blocks[0].ctx.from;
  return from == BlockFrom::CTO_FLUSH ||
         uploading_queue_->Size() < uploading_queue_->Capacity() * 0.5;
}

void BlockCacheUploader::ScaningWorker() {
  while (running_.load(std::memory_order_relaxed)) {
    auto stage_blocks = pending_queue_->Pop(true);  // peek it
    if (!CanUpload(stage_blocks)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      continue;
    }

    stage_blocks = pending_queue_->Pop();
    for (const auto& stage_block : stage_blocks) {
      uploading_queue_->Push(stage_block);
    }
  }
}

void BlockCacheUploader::UploadingWorker() {
  while (running_.load(std::memory_order_relaxed)) {
    auto stage_block = uploading_queue_->Pop();
    UploadStageBlock(stage_block);
  }
}

namespace {

void Log(const StageBlock& stage_block, size_t length, Errno rc,
         PhaseTimer timer) {
  auto message = StrFormat("upload_stage(%s,%d): %s%s <%.6lf>",
                           stage_block.key.Filename(), length, StrErr(rc),
                           timer.ToString(), timer.TotalUElapsed() / 1e6);
  LogIt(message);
}

};  // namespace

void BlockCacheUploader::UploadStageBlock(const StageBlock& stage_block) {
  Errno rc = Errno::OK;
  PhaseTimer timer;
  std::shared_ptr<char> buffer;
  size_t length;
  auto defer = ::absl::MakeCleanup([&]() {
    if (rc != Errno::OK) {
      Log(stage_block, length, rc, timer);
    }
  });

  timer.NextPhase(Phase::READ_BLOCK);
  rc = ReadBlock(stage_block, buffer, &length);
  if (rc == Errno::OK) {  // OK
    timer.NextPhase(Phase::S3_PUT);
    UploadBlock(stage_block, buffer, length, timer);
  } else if (rc == Errno::NOT_FOUND) {  // already deleted
    Uploaded(stage_block, false);
  } else {  // throw error
    Uploaded(stage_block, false);
  }
}

Errno BlockCacheUploader::ReadBlock(const StageBlock& stage_block,
                                    std::shared_ptr<char>& buffer,
                                    size_t* length) {
  auto stage_path = stage_block.stage_path;
  auto fs = LocalFileSystem();
  auto rc = fs.ReadFile(stage_path, buffer, length, FLAGS_drop_page_cache);
  if (rc == Errno::NOT_FOUND) {
    LOG(ERROR) << "Stage block (path=" << stage_path
               << ") already deleted, abort upload!";
  } else if (rc != Errno::OK) {
    LOG(ERROR) << "Read stage block (path=" << stage_path
               << ") failed: " << StrErr(rc) << ", abort upload!";
  }
  return rc;
}

void BlockCacheUploader::UploadBlock(const StageBlock& stage_block,
                                     std::shared_ptr<char> buffer,
                                     size_t length, PhaseTimer timer) {
  auto retry_cb = [stage_block, buffer, length, timer, this](int code) {
    auto key = stage_block.key;
    if (code != 0) {
      LOG(ERROR) << "Upload object " << key.Filename()
                 << " failed, code=" << code;
      return true;  // retry
    }

    RemoveBlock(stage_block);
    Uploaded(stage_block, true);
    Log(stage_block, length, Errno::OK, timer);
    return false;
  };
  s3_->AsyncPut(stage_block.key.StoreKey(), buffer.get(), length, retry_cb);
}

void BlockCacheUploader::RemoveBlock(const StageBlock& stage_block) {
  auto rc = store_->RemoveStage(stage_block.key, stage_block.ctx);
  if (rc != Errno::OK) {
    LOG(WARNING) << "Remove stage block (path=" << stage_block.stage_path
                 << ") after upload failed: " << StrErr(rc);
  }
}

void BlockCacheUploader::Staging(const StageBlock& stage_block) {
  if (NeedCount(stage_block)) {
    stage_count_->Add(stage_block.key.ino, 1, false);
  }
}

void BlockCacheUploader::Uploaded(const StageBlock& stage_block, bool success) {
  if (NeedCount(stage_block)) {
    stage_count_->Add(stage_block.key.ino, -1, !success);
  }
}

bool BlockCacheUploader::NeedCount(const StageBlock& stage_block) {
  return stage_block.ctx.from == BlockFrom::CTO_FLUSH;
}

void BlockCacheUploader::WaitAllUploaded() {
  while (pending_queue_->Size() != 0 || uploading_queue_->Size() != 0) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

}  // namespace blockcache
}  // namespace cache
}  // namespace dingofs
