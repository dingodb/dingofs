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

#include "cache/blockcache/aio_queue.h"

#include <glog/logging.h>

#include <atomic>
#include <memory>
#include <thread>

#include "cache/blockcache/aio.h"
#include "cache/blockcache/io_uring.h"
#include "cache/common/macro.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

DEFINE_uint32(iodepth, 128, "aio queue maximum iodepth");

AioQueue::AioQueue(const std::vector<iovec>& fixed_write_buffers,
                   const std::vector<iovec>& fixed_read_buffers)
    : running_(false),
      io_uring_(
          std::make_unique<IOUring>(fixed_write_buffers, fixed_read_buffers)),
      prep_io_queue_id_({0}) {}

Status AioQueue::Start() {
  if (running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "AioQueue already started";
    return Status::OK();
  }

  LOG(INFO) << "AioQueue is starting...";

  auto status = io_uring_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to start IOUring";
    return status;
  }

  bthread::ExecutionQueueOptions options;
  options.use_pthread = true;
  CHECK_EQ(0, bthread::execution_queue_start(&prep_io_queue_id_, &options,
                                             PrepareIO, this))
      << "Fail to start ExecutionQueue for prepare aio";

  bg_wait_thread_ = std::thread(&AioQueue::BackgroundWait, this);

  running_.store(true, std::memory_order_relaxed);
  LOG(INFO) << "AioQueue{iodepth=" << FLAGS_iodepth << "} is ready";
  return Status::OK();
}

Status AioQueue::Shutdown() {
  if (!running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "AioQueue already shutdown";
    return Status::OK();
  }

  LOG(INFO) << "AioQueue is shutting down...";

  CHECK_EQ(0, bthread::execution_queue_stop(prep_io_queue_id_));
  CHECK_EQ(0, bthread::execution_queue_join(prep_io_queue_id_));

  running_.store(false, std::memory_order_relaxed);
  bg_wait_thread_.join();
  CHECK(io_uring_->Shutdown().ok());

  LOG(INFO) << "AioQueue is down";
  return Status::OK();
}

void AioQueue::Submit(Aio* aio) {
  DCHECK_RUNNING("AioQueue");
  CHECK_EQ(0, bthread::execution_queue_execute(prep_io_queue_id_, aio));
}

int AioQueue::PrepareIO(void* meta, bthread::TaskIterator<Aio*>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  int n = 0;
  AioQueue* self = static_cast<AioQueue*>(meta);
  auto* prepared_aios = self->prepared_aios_;
  auto* io_uring = self->io_uring_.get();
  for (; iter; iter++) {
    auto* aio = *iter;
    Status status = io_uring->PrepareIO(aio);
    if (!status.ok()) {
      self->OnError(aio, status);
      continue;
    }

    prepared_aios[n++] = aio;
    if (n == kSubmitBatchSize) {
      self->BatchSubmitIO(prepared_aios, n);
      n = 0;
    }
  }

  if (n > 0) {
    self->BatchSubmitIO(prepared_aios, n);
  }
  return 0;
}

void AioQueue::BatchSubmitIO(Aio* aios[], int n) {
  Status status = io_uring_->SubmitIO();
  if (!status.ok()) {
    for (int i = 0; i < n; i++) {
      OnError(aios[i], status);
    }
    return;
  }
}

void AioQueue::BackgroundWait() {
  Aio* completed_aios[FLAGS_iodepth * 2];

  while (running_.load(std::memory_order_relaxed)) {
    int n = io_uring_->WaitIO(1000, completed_aios);
    if (n == 0) {
      continue;
    }

    CHECK_LE(n, FLAGS_iodepth * 2);

    for (int i = 0; i < n; i++) {
      OnComplete(completed_aios[i]);
    }
  }
}

void AioQueue::OnError(Aio* aio, Status status) {
  LOG(ERROR) << "Fail to run " << *aio;
  aio->status() = status;
  RunClosure(aio);
}

void AioQueue::OnComplete(Aio* aio) {
  if (!aio->status().ok()) {
    LOG(ERROR) << "Fail to run " << *aio;
  }
  RunClosure(aio);
}

// NOTE: The aio will been freed once closure runned
void AioQueue::RunClosure(Aio* aio) { aio->Run(); }

}  // namespace cache
}  // namespace dingofs
