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

#include "prefetch_manager.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <atomic>
#include <cstdint>
#include <memory>

#include "client/common/const.h"
#include "client/vfs/components/prefetch_utils.h"
#include "client/vfs/hub/vfs_hub.h"
#include "common/status.h"
#include "utils/executor/bthread/bthread_executor.h"

namespace dingofs {
namespace client {
namespace vfs {

using WriteLockGuard = dingofs::utils::WriteLockGuard;
using ReadLockGuard = dingofs::utils::ReadLockGuard;

int PrefetchManager::HandlePrefetch(
    void* meta, bthread::TaskIterator<PrefetchContext>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  auto* self = static_cast<PrefetchManager*>(meta);
  for (; iter; iter++) {
    auto& context = *iter;

    self->prefetch_executor_->Execute(
        [self, context]() { self->ProcessPrefetch(context); });
  }

  return 0;
}

Status PrefetchManager::Start(uint32_t threads) {
  CHECK_NOTNULL(metrics_);

  bthread::ExecutionQueueOptions queue_options;
  CHECK_EQ(0, bthread::execution_queue_start(&task_queue_, &queue_options,
                                             &PrefetchManager::HandlePrefetch,
                                             this));

  prefetch_executor_ = std::make_unique<BthreadExecutor>(threads);
  auto ok = prefetch_executor_->Start();
  if (!ok) {
    LOG(ERROR) << "Start prefetch manager executor failed.";
    return Status::Internal("Start prefetch manager executor failed.");
  }

  block_store_ = vfs_hub_->GetBlockStore();

  running_.store(true, std::memory_order_relaxed);

  LOG(INFO) << fmt::format("PrefetchManager started");
  return Status::OK();
}

Status PrefetchManager::Stop() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }

  CHECK_EQ(bthread::execution_queue_stop(task_queue_), 0);
  CHECK_EQ(bthread::execution_queue_join(task_queue_), 0);

  auto ok = prefetch_executor_->Stop();
  if (!ok) {
    LOG(ERROR) << "Stop prefetch executor failed.";
    return Status::Internal("Stop prefetch executor failed.");
  }

  running_.store(false, std::memory_order_relaxed);

  LOG(INFO) << "PrefetchManager stopped.";
  return Status::OK();
}

void PrefetchManager::SubmitTask(PrefetchContext context) {
  CHECK_EQ(0, bthread::execution_queue_execute(task_queue_, context));
}

void PrefetchManager::ProcessPrefetch(const PrefetchContext& context) {
  auto span = vfs_hub_->GetTracer()->StartSpan(kVFSDataMoudule, __func__);

  const auto block_size = vfs_hub_->GetFsInfo().block_size;
  // Prefetch include current block
  uint32_t prefetch_offset =
      (context.prefetch_offset / block_size) * block_size;
  uint32_t prefetch_max_len = std::min(context.prefetch_blocks * block_size,
                                       context.file_size - prefetch_offset);

  auto block_keys =
      FileRange2BlockKey(span->GetContext(), vfs_hub_, context.ino,
                         prefetch_offset, prefetch_max_len);

  const auto blocks = block_keys.size();
  if (blocks > context.prefetch_blocks)
    block_keys.erase(block_keys.end() - (blocks - context.prefetch_blocks),
                     block_keys.end());

  VLOG(9) << fmt::format(
      "Prefetch blocks for ino: {}, offset: {}, maxlen: {}, blocknums: {}.",
      context.ino, prefetch_offset, prefetch_max_len, block_keys.size());

  for (const auto& block : block_keys) {
    AsyncPrefetch(block.key, block.len);
  }
}

void PrefetchManager::AsyncPrefetch(BlockKey key, size_t length) {
  auto span = vfs_hub_->GetTracer()->StartSpan(
      kVFSDataMoudule, "PrefetchManager::AsyncPrefetch");

  if (IsBusy(key)) {
    VLOG(12) << "Skip block: " << key.Filename() << ", length: " << length;
    return;
  }

  VLOG(6) << "Try to prefetch block: " << key.Filename()
          << ", length: " << length;

  PrefetchReq req;
  req.block = key;
  req.block_size = length;

  auto ctx = span->GetContext();

  SetBusy(key);

  auto callback = [&, req, span_ptr = span.release()](const Status& status) {
    std::unique_ptr<ITraceSpan> span_guard(span_ptr);
    if (status.ok()) {
      VLOG(6) << "Prefetch block: " << req.block.Filename()
              << " finished, status = " << status.ToString();
    } else {
      LOG_EVERY_N(WARNING, 100)
          << "Prefetch failed: "
          << "key = " << req.block.Filename() << ", length = " << req.block_size
          << ", status = " << status.ToString();
    }

    SetIdle(req.block);
  };

  block_store_->PrefetchAsync(ctx, req, std::move(callback));
}

bool PrefetchManager::IsBusy(const BlockKey& key) {
  ReadLockGuard lk(rwlock_);
  return inflight_keys_.count(key.Filename()) != 0;
}

void PrefetchManager::SetBusy(const BlockKey& key) {
  WriteLockGuard lk(rwlock_);
  inflight_keys_.insert(key.Filename());
  metrics_->inflight_prefetch_blocks << 1;
}

void PrefetchManager::SetIdle(const BlockKey& key) {
  WriteLockGuard lk(rwlock_);
  inflight_keys_.erase(key.Filename());
  metrics_->inflight_prefetch_blocks << -1;
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs