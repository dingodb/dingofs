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
 * Created Date: 2025-02-10
 * Author: Jingli Chen (Wine93)
 */

#include "cache/cachegroup/cache_group_node.h"

#include <brpc/reloadable_flags.h>
#include <butil/binary_printer.h>
#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <atomic>
#include <memory>

#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/block_cache_impl.h"
#include "cache/cachegroup/task_tracker.h"
#include "cache/common/const.h"
#include "cache/common/macro.h"
#include "cache/utils/context.h"
#include "cache/utils/helper.h"
#include "cache/utils/step_timer.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

DEFINE_string(id, "", "Specified the cache node id (only mds v2 required)");

DEFINE_string(group_name, "", "Which group this cache node belongs to");
DEFINE_validator(group_name, Helper::NonEmptyString);

DEFINE_string(listen_ip, "",
              "IP address to listen on for this cache group node");
DEFINE_validator(listen_ip, Helper::NonEmptyString);

DEFINE_uint32(listen_port, 9300, "Port to listen on for this cache group node");

DEFINE_uint32(group_weight, 100,
              "Weight of this cache group node, used for consistent hashing");

DEFINE_uint32(max_range_size_kb, 128,
              "Retrieve the whole block if length of range request is larger "
              "than this value");

DEFINE_bool(retrieve_storage_lock, true, "Lock when retrieve from storage");

DEFINE_uint32(retrieve_storage_lock_timeout_ms, 10000,
              "Timeout of retrieve from storage lock");
DEFINE_validator(retrieve_storage_lock_timeout_ms, brpc::PassValidate);

static const std::string kModule = "cachenode";

CacheGroupNodeImpl::CacheGroupNodeImpl()
    : running_(false),
      mds_client_(BuildSharedMDSClient()),
      member_(std::make_shared<CacheGroupNodeMember>(mds_client_)),
      heartbeat_(
          std::make_unique<CacheGroupNodeHeartbeat>(member_, mds_client_)),
      storage_pool_(std::make_shared<StoragePoolImpl>(mds_client_)),
      task_tracker_(std::make_unique<TaskTracker>()),
      metric_cache_hit_count_("dingofs_cache_hit_count"),
      metric_cache_miss_count_("dingofs_cache_miss_count") {}

Status CacheGroupNodeImpl::Start() {
  CHECK_NOTNULL(mds_client_);
  CHECK_NOTNULL(member_);
  CHECK_NOTNULL(heartbeat_);
  CHECK_NOTNULL(storage_pool_);

  if (running_) {
    return Status::OK();
  }

  LOG(INFO) << "Cache group node is starting...";

  auto status = mds_client_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Start MDS client failed: " << status.ToString();
    return status;
  }

  status = member_->JoinGroup();
  if (!status.ok()) {
    LOG(ERROR) << "Join node to cache group failed: " << status.ToString();
    return status;
  }

  SCOPE_EXIT {
    if (!status.ok()) {
      member_->LeaveGroup();
    }
  };

  // Cache directory name depends node member uuid, so init after join group
  status = StartBlockCache();
  if (!status.ok()) {
    LOG(ERROR) << "Init block cache failed: " << status.ToString();
    return status;
  }

  heartbeat_->Start();

  running_ = true;

  LOG(INFO) << "Cache group node is up.";

  CHECK_RUNNING("Cache group node");
  return Status::OK();
}

Status CacheGroupNodeImpl::Shutdown() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }

  LOG(INFO) << "Cache group node is shutting down...";

  heartbeat_->Shutdown();

  Status status = member_->LeaveGroup();
  if (!status.ok()) {
    LOG(ERROR) << "Leave cache group failed: " << status.ToString();
    return status;
  }

  status = block_cache_->Shutdown();
  if (!status.ok()) {
    LOG(ERROR) << "Shutdown block cache failed: " << status.ToString();
    return status;
  }

  LOG(INFO) << "Cache group node is down.";

  CHECK_DOWN("Cache group node");
  return status;
}

Status CacheGroupNodeImpl::StartBlockCache() {
  auto member_id = member_->GetMemberId();
  CHECK(!member_id.empty()) << "Member id should not be empty";
  FLAGS_cache_dir_uuid = member_id;

  block_cache_ = std::make_shared<BlockCacheImpl>(storage_pool_);
  return block_cache_->Start();
}

Status CacheGroupNodeImpl::Put(ContextSPtr ctx, const BlockKey& key,
                               const Block& block, PutOption option) {
  if (!IsRunning()) {
    return Status::Internal("cache group node is not running");
  }

  Status status;
  StepTimer timer;
  TraceLogGuard log(ctx, status, timer, kModule, "put(%s,%zu)", key.Filename(),
                    block.size);
  StepTimerGuard guard(timer);

  NEXT_STEP("local_put");
  status = block_cache_->Put(ctx, key, block, option);

  return status;
}

Status CacheGroupNodeImpl::Range(ContextSPtr ctx, const BlockKey& key,
                                 off_t offset, size_t length, IOBuffer* buffer,
                                 RangeOption option) {
  if (!IsRunning()) {
    return Status::Internal("cache group node is not running");
  }

  Status status;
  StepTimer timer;
  TraceLogGuard log(ctx, status, timer, kModule, "range(%s,%lld,%zu)",
                    key.Filename(), offset, length);
  StepTimerGuard guard(timer);

  status = RetrieveCache(ctx, timer, key, offset, length, buffer, option);
  if (status.IsNotFound()) {
    status = RetrieveStorage(ctx, timer, key, offset, length, buffer, option);
  }
  return status;
}

void CacheGroupNodeImpl::AsyncCache(ContextSPtr ctx, const BlockKey& key,
                                    const Block& block, AsyncCallback callback,
                                    CacheOption option) {
  if (!IsRunning()) {
    callback(Status::Internal("cache group node is not running"));
    return;
  }

  auto timer = std::make_shared<StepTimer>();
  timer->Start();

  auto cb = [timer, ctx, key, block, callback](Status status) {
    TraceLogGuard log(ctx, status, *timer, kModule, "async_cache(%s,%zu)",
                      key.Filename(), block.size);
    callback(status);
    timer->Stop();
  };

  block_cache_->AsyncCache(ctx, key, block, cb, option);
}

void CacheGroupNodeImpl::AsyncPrefetch(ContextSPtr ctx, const BlockKey& key,
                                       size_t length, AsyncCallback callback,
                                       PrefetchOption option) {
  if (!IsRunning()) {
    callback(Status::Internal("cache group node is not running"));
    return;
  }

  auto timer = std::make_shared<StepTimer>();
  timer->Start();

  auto cb = [timer, ctx, key, length, callback](Status status) {
    TraceLogGuard log(ctx, status, *timer, kModule, "async_prefetch(%s,%zu)",
                      key.Filename(), length);
    callback(status);
    timer->Stop();
  };

  block_cache_->AsyncPrefetch(ctx, key, length, cb, option);
}

Status CacheGroupNodeImpl::RetrieveCache(ContextSPtr ctx, StepTimer& timer,
                                         const BlockKey& key, off_t offset,
                                         size_t length, IOBuffer* buffer,
                                         RangeOption option) {
  NEXT_STEP("local_range");
  option.retrive = false;
  auto status = block_cache_->Range(ctx, key, offset, length, buffer, option);
  if (status.ok()) {
    metric_cache_hit_count_ << 1;
    ctx->SetCacheHit(true);
  } else {
    metric_cache_miss_count_ << 1;
  }
  return status;
}

Status CacheGroupNodeImpl::RetrieveStorage(ContextSPtr ctx, StepTimer& timer,
                                           const BlockKey& key, off_t offset,
                                           size_t length, IOBuffer* buffer,
                                           RangeOption option) {
  NEXT_STEP("get_storage")
  StorageSPtr storage;
  auto status = storage_pool_->GetStorage(key.fs_id, storage);
  if (!status.ok()) {
    return status;
  }

  // Retrieve range of block: unknown block size or unreach max_range_size
  auto block_whole_length = option.block_size;
  if (block_whole_length == 0 || length <= FLAGS_max_range_size_kb * kKiB) {
    return RetrievePartBlock(ctx, timer, storage, key, offset, length,
                             block_whole_length, buffer);
  }

  // Retrieve the whole block
  IOBuffer block;
  status =
      RetrieveWholeBlock(ctx, timer, storage, key, block_whole_length, &block);
  if (status.ok()) {
    block.AppendTo(buffer, length, offset);
  }
  return status;
}

Status CacheGroupNodeImpl::RetrievePartBlock(
    ContextSPtr ctx, StepTimer& timer, StorageSPtr storage, const BlockKey& key,
    off_t offset, size_t length, size_t block_whole_length, IOBuffer* buffer) {
  NEXT_STEP("s3_range")
  auto status = storage->Range(ctx, key, offset, length, buffer);
  if (status.ok() && block_whole_length > 0) {
    NEXT_STEP("async_prefetch")
    block_cache_->AsyncPrefetch(ctx, key, block_whole_length,
                                [](Status status) {});
  }
  return status;
}

Status CacheGroupNodeImpl::RetrieveWholeBlock(ContextSPtr ctx, StepTimer& timer,
                                              StorageSPtr storage,
                                              const BlockKey& key,
                                              size_t length, IOBuffer* buffer) {
  if (!FLAGS_retrieve_storage_lock) {
    NEXT_STEP("s3_get")
    return storage->Range(ctx, key, 0, length, buffer);
  }

  DownloadTaskSPtr task;
  bool created =
      task_tracker_->GetOrCreateTask(ctx, storage, key, length, buffer, task);
  if (created) {
    NEXT_STEP("s3_get")
    auto status = task->Run(true);
    if (!status.ok()) {
      task_tracker_->RemoveTask(key);
    } else {
      block_cache_->AsyncCache(
          task->ctx, task->key, Block(*task->buffer), [&](Status status) {
            auto ctx = task->ctx;
            if (!status.ok()) {
              LOG_CTX(ERROR)
                  << "Async cache failed: key = " << task->key.Filename()
                  << ", status = " << status.ToString();
            }
            task_tracker_->RemoveTask(task->key);
          });
    }

    return status;
  }

  return WaitTask(timer, task);
}

Status CacheGroupNodeImpl::RunTask(StepTimer& timer, DownloadTaskSPtr task) {
  NEXT_STEP("s3_get")
  auto status = task->Run(true);
  if (!status.ok()) {
    task_tracker_->RemoveTask(task->key);
    return status;
  }

  NEXT_STEP("async_cache")
  block_cache_->AsyncCache(
      task->ctx, task->key, Block(*task->buffer), [task](Status status) {
        auto ctx = task->ctx;
        if (!status.ok()) {
          LOG_CTX(ERROR) << "Async cache failed: key = " << task->key.Filename()
                         << ", status = " << status.ToString();
        }

        // task_tracker->RemoveTask(task->key);
      });
  return Status::OK();
}

Status CacheGroupNodeImpl::WaitTask(DownloadTaskSPtr task) {
  bool finished = task->Wait(FLAGS_retrieve_storage_lock_timeout_ms);
  if (finished) {
    auto status = task->status;
    if (status.ok() || status.IsNotFound()) {
      return status;
    }
  }

  // not finished or finished with other status, so will re-run the task
  auto status = task->Run(false);
  if (status.ok()) {
    AsyncCache(task);
  }
  return status;
}

void CacheGroupNodeImpl::AsyncCache(DownloadTaskSPtr task) {
  block_cache_->AsyncCache(
      task->ctx, task->key, Block(*task->buffer), [task](Status status) {
        auto ctx = task->ctx;
        if (!status.ok()) {
          LOG_CTX(ERROR) << "Async cache failed: key = " << task->key.Filename()
                         << ", status = " << status.ToString();
        }
      });
}

}  // namespace cache
}  // namespace dingofs
