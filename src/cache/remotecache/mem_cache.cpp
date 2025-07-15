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

#include "cache/remotecache/mem_cache.h"

#include <absl/strings/str_format.h>
#include <butil/iobuf.h>
#include <butil/time.h>
#include <glog/logging.h>

#include "cache/blockcache/cache_store.h"
#include "cache/common/const.h"
#include "cache/common/macro.h"
#include "cache/common/type.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

MemCacheImpl::MemCacheImpl(size_t capacity)
    : running_(false),
      used_(0),
      capacity_(capacity),
      lru_(std::make_unique<utils::LRUCache<std::string, Block>>(1LL << 62)) {}

Status MemCacheImpl::Start() {
  CHECK_NOTNULL(lru_);
  CHECK_EQ(used_, 0);
  CHECK_GT(capacity_, 0);
  CHECK_EQ(lru_->Size(), 0);

  if (running_) {
    return Status::OK();
  }

  LOG(INFO) << "MemCache is starting...";

  running_ = true;

  LOG(INFO) << "MemCache is up.";

  CHECK_RUNNING("MemCache");
  return Status::OK();
}

Status MemCacheImpl::Shutdown() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }

  LOG(INFO) << "MemCache is shutting down...";
  // do nothing
  LOG(INFO) << "MemCache is down.";

  CHECK_DOWN("MemCache");
  return Status::OK();
}

void MemCacheImpl::Put(const BlockKey& key, const Block& block) {
  if (Exist(key)) {
    return;
  }

  lru_->Put(key.Filename(), block);
  UpdateUsage(block.size);
}

Status MemCacheImpl::Get(const BlockKey& key, Block* block) {
  bool yes = lru_->Get(key.Filename(), block);
  if (!yes) {
    return Status::NotFound("cache not found");
  }
  return Status::OK();
}

bool MemCacheImpl::Exist(const BlockKey& key) {
  Block block;
  return lru_->Get(key.Filename(), &block);
}

void MemCacheImpl::UpdateUsage(size_t add_bytes) {
  std::unique_lock<BthreadMutex> lock(mutex_);
  used_ += add_bytes;
  if (used_ > capacity_) {
    CleanupFull(capacity_ * 0.95);
  }
}

void MemCacheImpl::CleanupFull(size_t goal_bytes) {
  butil::Timer timer;
  size_t num_evicted = 0;
  size_t free_bytes = 0;

  timer.start();

  std::string key;
  Block block;
  while (used_ > goal_bytes && lru_->GetLast(&key, &block)) {
    lru_->Remove(key);
    used_ -= block.size;
    num_evicted++;
    free_bytes += block.size;
  }

  timer.stop();

  LOG(INFO) << absl::StrFormat(
      "%d blocks deleted, free %.2lf MiB, cost %.6lf seconds", num_evicted,
      free_bytes / kMiB, timer.u_elapsed() * 1.0 / 1e6);
}

};  // namespace cache
};  // namespace dingofs
