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
 * Created Date: 2025-07-15
 * Author: Jingli Chen (Wine93)
 */

#include "cache/remotecache/prefetcher.h"

#include <butil/iobuf.h>
#include <glog/logging.h>

#include <memory>

#include "cache/common/macro.h"
#include "cache/utils/bthread.h"

namespace dingofs {
namespace cache {

Prefetcher::Prefetcher(MemCacheSPtr mem_cache, RemoteCacheNodeSPtr remote_node)
    : running_(false),
      memcache_(mem_cache),
      remote_node_(remote_node),
      queue_id_({0}),
      joiner_(std::make_unique<BthreadJoiner>()) {}

Status Prefetcher::Start() {
  CHECK_NOTNULL(memcache_);
  CHECK_NOTNULL(remote_node_);
  CHECK_NOTNULL(joiner_);
  CHECK(busy_.empty());

  if (running_) {
    return Status::OK();
  }

  LOG(INFO) << "Prefetcher is starting...";

  bthread::ExecutionQueueOptions queue_options;
  queue_options.use_pthread = true;
  int rc = bthread::execution_queue_start(&queue_id_, &queue_options,
                                          HandleTask, this);
  if (rc != 0) {
    LOG(ERROR) << "Start execution queue failed: rc = " << rc;
    return Status::Internal("start execution queue failed");
  }

  auto status = joiner_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Start joiner failed: " << status.ToString();
    return status;
  }

  running_ = true;

  LOG(INFO) << "Prefetcher is up.";

  CHECK_RUNNING("Prefetcher");
  return Status::OK();
}

Status Prefetcher::Shutdown() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }

  LOG(INFO) << "Prefetcher is shutting down...";

  if (bthread::execution_queue_stop(queue_id_) != 0) {
    LOG(ERROR) << "Stop execution queue failed.";
    return Status::Internal("stop execution queue failed");
  } else if (bthread::execution_queue_join(queue_id_) != 0) {
    LOG(ERROR) << "Join execution queue failed.";
    return Status::Internal("join execution queue failed");
  }

  auto status = joiner_->Shutdown();
  if (!status.ok()) {
    LOG(ERROR) << "Shutdown joiner failed: " << status.ToString();
    return status;
  }

  busy_.clear();

  LOG(INFO) << "Prefetcher is down.";

  CHECK_DOWN("Prefetcher");
  return Status::OK();
}

void Prefetcher::Submit(ContextSPtr ctx, const BlockKey& key, size_t length) {
  CHECK_GT(length, 0);
  CHECK_EQ(0,
           bthread::execution_queue_execute(queue_id_, Task(ctx, key, length)));
}

int Prefetcher::HandleTask(void* meta, bthread::TaskIterator<Task>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  auto* self = static_cast<Prefetcher*>(meta);
  for (; iter; iter++) {
    auto& task = *iter;
    // Skip if the key is already being processed or exists in cache
    if (self->FilterOut(task)) {
      continue;
    }

    self->SetBusy(task.key);
    auto tid = RunInBthread([self, task]() { self->DoPrefetch(task); });
    if (tid != 0) {
      self->joiner_->BackgroundJoin(tid);
    }
  }

  return 0;
}

void Prefetcher::DoPrefetch(const Task& task) {
  SCOPE_EXIT { SetIdle(task.key); };
  const auto& ctx = task.ctx;
  IOBuffer buffer;
  RangeOption option;
  option.block_size = task.length;
  auto status =
      remote_node_->Range(ctx, task.key, 0, task.length, &buffer, option);
  if (!status.ok()) {
    LOG_EVERY_N_CTX(WARNING, 100)
        << "Prefetch failed: "
        << "key = " << task.key.Filename() << ", length = " << task.length
        << ", status = " << status.ToString();
    return;
  }

  memcache_->Put(task.key, Block(buffer));
}

bool Prefetcher::IsBusy(const BlockKey& key) {
  ReadLockGuard lk(rwlock_);
  return busy_.count(key.Filename()) != 0;
}

void Prefetcher::SetBusy(const BlockKey& key) {
  WriteLockGuard lk(rwlock_);
  busy_.insert(key.Filename());
}

void Prefetcher::SetIdle(const BlockKey& key) {
  WriteLockGuard lk(rwlock_);
  busy_.erase(key.Filename());
}

bool Prefetcher::FilterOut(const Task& task) {
  return IsBusy(task.key) || memcache_->Exist(task.key);
}

};  // namespace cache
};  // namespace dingofs
