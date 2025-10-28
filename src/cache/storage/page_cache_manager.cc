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
 * Created Date: 2025-03-17
 * Author: Jingli Chen (Wine93)
 */

#include "cache/storage/page_cache_manager.h"

#include <fcntl.h>
#include <unistd.h>

#include <atomic>

#include "cache/common/macro.h"
#include "cache/utils/context.h"
#include "cache/utils/posix.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

PageCacheManager::PageCacheManager() : running_(false), queue_id_({0}) {}

Status PageCacheManager::Start() {
  if (running_) {
    return Status::OK();
  }

  LOG(INFO) << "Page cache manager is starting...";

  bthread::ExecutionQueueOptions queue_options;
  queue_options.use_pthread = true;
  int rc = bthread::execution_queue_start(&queue_id_, &queue_options,
                                          HandleTask, this);
  if (rc != 0) {
    LOG(ERROR) << "Start execution queue failed: rc = " << rc;
    return Status::Internal("start execution queue failed");
  }

  running_ = true;
  LOG(INFO) << "Page cache manager is up.";

  CHECK_RUNNING("Page cache manager");
  return Status::OK();
}

Status PageCacheManager::Shutdown() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }

  LOG(INFO) << "Page cache manager is shutting down...";

  if (bthread::execution_queue_stop(queue_id_) != 0) {
    LOG(ERROR) << "Stop execution queue failed";
    return Status::Internal("stop execution queue failed");
  } else if (bthread::execution_queue_join(queue_id_) != 0) {
    LOG(ERROR) << "Join execution queue failed.";
    return Status::Internal("join execution queue failed");
  }

  LOG(INFO) << "Page cache manager is down.";

  CHECK_DOWN("Page cache manager");
  return Status::OK();
}

void PageCacheManager::AsyncDropPageCache(ContextSPtr ctx, int fd, off_t offset,
                                          size_t length, bool sync_data) {
  if (!IsRunning()) {
    LOG_CTX(WARNING)
        << "Page cache manager is not running, abort drop page cache: fd = "
        << fd << ", offset = " << offset << ", length = " << length
        << ", sync_data = " << sync_data;
    return;
  }

  Task task(ctx, fd, offset, length, sync_data);
  CHECK_EQ(0, bthread::execution_queue_execute(queue_id_, task));
}

int PageCacheManager::HandleTask(void* meta,
                                 bthread::TaskIterator<Task>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  auto* self = static_cast<PageCacheManager*>(meta);
  for (; iter; iter++) {
    self->Handle(*iter);
  }
  return 0;
}

void PageCacheManager::Handle(const Task& task) {
  const auto& ctx = task.ctx;
  VLOG_CTX(9) << "Drop page cache: fd = " << task.fd
              << ", offset = " << task.offset << ", length = " << task.length
              << ", sync = " << task.sync_data;

  if (task.sync_data) {
    SyncData(ctx, task.fd);
  }

  DropCache(ctx, task.fd, task.offset, task.length);
  CloseFd(ctx, task.fd);
}

void PageCacheManager::SyncData(ContextSPtr ctx, int fd) {
  auto status = Posix::FSync(fd);
  if (!status.ok()) {
    LOG_CTX(WARNING) << "Sync data failed: fd = " << fd
                     << ", status = " << status.ToString();
  }
}

void PageCacheManager::DropCache(ContextSPtr ctx, int fd, off_t offset,
                                 size_t length) {
  auto status = Posix::PosixFAdvise(fd, offset, length, POSIX_FADV_DONTNEED);
  if (!status.ok()) {
    LOG_CTX(WARNING) << "Drop page cache failed: fd = " << fd
                     << ", status = " << status.ToString();
  }
}

void PageCacheManager::CloseFd(ContextSPtr ctx, int fd) {
  auto status = Posix::Close(fd);
  if (!status.ok()) {
    LOG_CTX(ERROR) << "Close file descriptor failed: fd = " << fd
                   << ", status = " << status.ToString();
  }
}

}  // namespace cache
}  // namespace dingofs
