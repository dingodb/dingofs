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
 * Created Date: 2025-03-30
 * Author: Jingli Chen (Wine93)
 */

#include "client/blockcache/aio_queue.h"

#include <glog/logging.h>

#include <thread>

#include "client/blockcache/error.h"

namespace dingofs {
namespace client {
namespace blockcache {

AioQueueImpl::AioQueueImpl(const std::string& mountpoint, bool for_read)
    : running_(false),
      mountpoint_(mountpoint),
      for_read_(for_read),
      io_uring_(),
      submit_queue_id_({0}) {}

bool AioQueueImpl::Init(uint32_t io_depth) {
  if (running_.exchange(true)) {  // already running
    return true;
  }

  int rc = hf3fs_iorcreate4(&io_uring_, mountpoint_.c_str(), 4096, for_read_,
                            io_depth, 0, -1, 0);
  if (rc != 0) {
    LOG(ERROR) << "Create hf3fs ior failed, rc = " << rc;
    return false;
  }

  bthread::ExecutionQueueOptions queue_options;
  queue_options.use_pthread = true;
  rc = bthread::execution_queue_start(&submit_queue_id_, &queue_options,
                                      BatchSubmit, this);
  if (rc != 0) {
    LOG(ERROR) << "execution_queue_start() failed, rc=" << rc;
    return false;
  }

  thread_ = std::thread(&AioQueueImpl::BackgroundWait, this);
  return true;
}

bool AioQueueImpl::Shutdown() {
  if (running_.exchange(false)) {
    thread_.join();
    bthread::execution_queue_stop(submit_queue_id_);
    int rc = bthread::execution_queue_join(submit_queue_id_);
    if (rc == 0) {
      hf3fs_iordestroy(&io_uring_);
      return true;
    }
    return false;
  }
  return true;
}

void AioQueueImpl::Submit(BlockTask* task) {
  CHECK_EQ(0, bthread::execution_queue_execute(submit_queue_id_, task));
}

int AioQueueImpl::BatchSubmit(void* meta,
                              bthread::TaskIterator<BlockTask*>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  std::vector<BlockTask*> fail_tasks, succ_tasks;
  AioQueueImpl* queue = static_cast<AioQueueImpl*>(meta);
  for (; iter; iter++) {
    auto* task = *iter;
    bool succ = queue->PrepareIO(task);
    if (succ) {
      succ_tasks.emplace_back(task);
    } else {
      fail_tasks.emplace_back(task);
    }
  }

  OnError(fail_tasks);
  int rc = hf3fs_submit_ios(&queue->io_uring_);
  if (rc != 0) {
    OnError(succ_tasks);
  }
  return 0;
}

bool AioQueueImpl::PrepareIO(BlockTask* task) {
  int rc = hf3fs_reg_fd(task->fd, 0);
  if (rc != 0) {
    return false;
  }

  rc = hf3fs_iovcreate(&task->iov, mountpoint_.c_str(), task->length,
                       task->block_size, -1);
  if (rc != 0) {
    return false;
  }

  off_t offset = 0;
  size_t length = task->length;
  size_t block_size = task->block_size;
  while (length > 0) {
    size_t nbytes = std::min(length, block_size);
    int rc = hf3fs_prep_io(&io_uring_, &task->iov, for_read_,
                           task->iov.base + offset, task->fd,
                           task->offset + offset, nbytes, task);
    if (rc < 0) {
      return false;
    }

    length -= nbytes;
    offset += nbytes;
  }
  return true;
}

void AioQueueImpl::BackgroundWait() {
  hf3fs_cqe completed_entries[512];

  while (running_.load(std::memory_order_relaxed)) {
    int n = hf3fs_wait_for_ios(&io_uring_, completed_entries, 512, 1, nullptr);
    if (n < 0) {
      LOG(ERROR) << "Wait for aio failed: rc = " << n;
      continue;
    }
    RunClosure(completed_entries, n);
  }
}

void AioQueueImpl::OnError(const std::vector<BlockTask*>& tasks) {
  for (const auto* task : tasks) {
    task->done->SetCode(BCACHE_ERROR::IO_ERROR);
    task->done->Run();  // TODO: run in bthread
  }
}

void AioQueueImpl::OnSuccess(const std::vector<BlockTask*>& tasks) {
  for (const auto* task : tasks) {
    task->done->SetCode(BCACHE_ERROR::OK);
    task->done->Run();  // TODO: run in bthread
  }
}

void AioQueueImpl::RunClosure(const hf3fs_cqe* completed_entries, int n) {
  for (int i = 0; i < n; i++) {
    auto* task = (struct BlockTask*)(completed_entries[i].userdata);
    if (completed_entries[i].result == 0) {
      task->done->SetCode(BCACHE_ERROR::OK);
    } else {
      task->done->SetCode(BCACHE_ERROR::IO_ERROR);
    }
    task->done->Run();  // FIXME: run in bthread
  }
}

}  // namespace blockcache
}  // namespace client
}  // namespace dingofs