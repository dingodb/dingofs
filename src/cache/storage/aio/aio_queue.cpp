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

#include "cache/storage/aio/aio_queue.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_format.h>

#include "cache/utils/access_log.h"

namespace dingofs {
namespace cache {

AioQueueImpl::AioQueueImpl(std::shared_ptr<IORing> io_ring)
    : running_(false),
      ioring_(io_ring),
      prep_io_queue_id_({0}),
      infight_throttle_(io_ring->GetIODepth()) {}

Status AioQueueImpl::Init() {
  if (running_.exchange(true)) {  // already running
    return Status::OK();
  }

  Status status = ioring_->Init();
  if (!status.ok()) {
    LOG(ERROR) << "Init io ring failed: status = " << status.ToString();
    return status;
  }

  bthread::ExecutionQueueOptions options;
  options.use_pthread = true;
  int rc = bthread::execution_queue_start(&prep_io_queue_id_, &options,
                                          PrepareIO, this);
  if (rc != 0) {
    LOG(ERROR) << "Start aio prepare execution queue failed: rc = " << rc;
    return Status::Internal("bthread::execution_queue_start() failed");
  }

  bg_wait_thread_ = std::thread(&AioQueueImpl::BackgroundWait, this);
  return Status::OK();
}

Status AioQueueImpl::Shutdown() {
  if (running_.exchange(false)) {
    bthread::execution_queue_stop(prep_io_queue_id_);
    int rc = bthread::execution_queue_join(prep_io_queue_id_);
    if (rc != 0) {
      LOG(ERROR) << "Join aio prepare execution queue failed: rc = " << rc;
      return Status::Internal("bthread::execution_queue_join() failed");
    }
    bg_wait_thread_.join();

    return ioring_->Shutdown();
  }
  return Status::OK();
}

void AioQueueImpl::EnterPhase(AioClosure* aio, Phase phase) {
  aio->timer.NextPhase(phase);
}

void AioQueueImpl::BatchEnterPhase(AioClosure* aios[], int n, Phase phase) {
  for (int i = 0; i < n; i++) {
    aios[i]->timer.NextPhase(phase);
  }
}

void AioQueueImpl::Submit(AioClosure* aio) {
  EnterPhase(aio, Phase::kWaitThrottle);
  infight_throttle_.Increment(1);

  EnterPhase(aio, Phase::kCheckIO);
  CheckIO(aio);
}

void AioQueueImpl::CheckIO(AioClosure* aio) {
  if (aio->fd <= 0 || aio->length < 0) {
    OnError(aio);
    return;
  }

  EnterPhase(aio, Phase::kEnterPrepareQueue);
  CHECK_EQ(0, bthread::execution_queue_execute(prep_io_queue_id_, aio));
}

int AioQueueImpl::PrepareIO(void* meta,
                            bthread::TaskIterator<AioClosure*>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  int n = 0;
  static AioClosure* aios[kSubmitBatchSize];
  AioQueueImpl* queue = static_cast<AioQueueImpl*>(meta);
  auto ioring = queue->ioring_;
  for (; iter; iter++) {
    auto* aio = *iter;
    EnterPhase(aio, Phase::kPrepareIO);
    Status status = ioring->PrepareIO(aio);
    if (!status.ok()) {
      queue->OnError(aio);
      continue;
    }

    aios[n++] = aio;
    if (n == kSubmitBatchSize) {
      BatchEnterPhase(aios, n, Phase::kSubmitIO);
      queue->BatchSubmitIO(aios, n);
      n = 0;
    }
  }

  if (n > 0) {
    BatchEnterPhase(aios, n, Phase::kSubmitIO);
    queue->BatchSubmitIO(aios, n);
  }
  return 0;
}

void AioQueueImpl::BatchSubmitIO(AioClosure* aios[], int n) {
  Status status = ioring_->SubmitIO();
  for (int i = 0; i < n; i++) {
    if (status.ok()) {
      EnterPhase(aios[i], Phase::kExecuteIO);
    } else {
      OnError(aios[i]);
    }
  }
}

void AioQueueImpl::BackgroundWait() {
  std::vector<AioClosure*> completed;
  while (running_.load(std::memory_order_relaxed)) {
    Status status = ioring_->WaitIO(1000, &completed);
    if (!status.ok() || completed.empty()) {
      continue;
    }

    for (auto* aio : completed) {
      OnCompleted(aio);
    }
  }
}

Status AioQueueImpl::GetErrorStatus(AioClosure* aio) {
  auto phase = aio->timer.GetPhase();
  switch (phase) {
    case Phase::kCheckIO:
      return Status::InvalidParam("invalid aio param");
    case Phase::kPrepareIO:
      return Status::Internal("prepare aio failed");
    case Phase::kSubmitIO:
      return Status::Internal("submit aio failed");
    default:
      CHECK(false) << "Invalid aio phase : " << static_cast<int>(phase);
  }
}

void AioQueueImpl::OnError(AioClosure* aio) {
  auto phase = aio->timer.GetPhase();
  // TODO: CHECK_LT(phase, Phase::kExecuteIO);

  auto status = GetErrorStatus(aio);
  aio->status() = status;
  RunClosure(aio);
}

void AioQueueImpl::OnCompleted(AioClosure* aio) {
  // NOTE: The status of completed aio is set in the io_ring.
  CHECK(aio->timer.GetPhase() == Phase::kExecuteIO);
  RunClosure(aio);
}

// TODO(Wine93): run in bthread
void AioQueueImpl::RunClosure(AioClosure* aio) {
  auto status = aio->status();
  auto timer = aio->timer;
  auto description = aio->ToString();

  // error log
  if (!status.ok()) {
    LOG(ERROR) << "Aio error raised for " << description << " in "
               << StrPhase(timer.GetPhase()) << " phase: " << status.ToString();
  }

  // run closure
  aio->Run();

  // access log
  LogIt(absl::StrFormat("%s: %s%s <.6lf>", description, status.ToString(),
                        timer.ToString()));

  infight_throttle_.Decrement(1);
}

}  // namespace cache
}  // namespace dingofs
