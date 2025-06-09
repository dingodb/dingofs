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

#include <cstdint>
#include <memory>

#include "cache/storage/aio/aio.h"
#include "cache/utils/access_log.h"
#include "cache/utils/phase_timer.h"

namespace dingofs {
namespace cache {

AioQueueImpl::AioQueueImpl(std::shared_ptr<IORing> io_ring)
    : running_(false),
      ioring_(io_ring),
      prep_io_queue_id_({0}),
      prep_aios_(kSubmitBatchSize),
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

  LOG(INFO) << "Aio queue started: iodepth = " << ioring_->GetIODepth();

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

void AioQueueImpl::BatchEnterPhase(const Aios& aios, int n, Phase phase) {
  for (int i = 0; i < n; i++) {
    EnterPhase(aios[i], phase);
  }
}

uint64_t AioQueueImpl::GetTotalSize(const Aios& aios, int n) {
  uint64_t total_size = 0;
  for (int i = 0; i < n; i++) {
    total_size += aios[i]->length;
  }
  return total_size;
}

void AioQueueImpl::Submit(AioClosure* aio) {
  EnterPhase(aio, Phase::kWaitThrottle);
  infight_throttle_.Increment(1);

  LOG(INFO) << "inflights: " << infight_throttle_.GetInflights();

  EnterPhase(aio, Phase::kCheckIO);
  CheckIO(aio);
  // CHECK_EQ(0, bthread::execution_queue_execute(prep_io_queue_id_, aio));
}

void AioQueueImpl::CheckIO(AioClosure* aio) {
  if (aio->fd <= 0 || aio->length < 0) {
    OnError(aio, Status::Internal("invalid aio param"));
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
  AioQueueImpl* queue = static_cast<AioQueueImpl*>(meta);
  std::vector<AioClosure*> aios(kSubmitBatchSize);

  auto ioring = queue->ioring_;
  for (; iter; iter++) {
    auto* aio = *iter;
    EnterPhase(aio, Phase::kPrepareIO);
    Status status = ioring->PrepareIO(aio);
    if (!status.ok()) {
      queue->OnError(aio, status);
      continue;
    }

    aios[n++] = aio;
    if (n == kSubmitBatchSize) {
      BatchEnterPhase(aios, n, Phase::kExecuteIO);
      queue->BatchSubmitIO(aios, n);
      n = 0;
    }
  }

  if (n > 0) {
    BatchEnterPhase(aios, n, Phase::kExecuteIO);
    queue->BatchSubmitIO(aios, n);
  }
  return 0;
}

void AioQueueImpl::BatchSubmitIO(const Aios& aios, int n) {
  Status status = ioring_->SubmitIO();
  if (!status.ok()) {
    for (auto* aio : aios) {
      OnError(aio, status);
    }
    return;
  }

  VLOG(9) << n << " aio[s] submitted: batch size = " << GetTotalSize(aios, n);
}

void AioQueueImpl::BackgroundWait() {
  Aios completed;
  while (running_.load(std::memory_order_relaxed)) {
    Status status = ioring_->WaitIO(1000, &completed);
    if (!status.ok() || completed.empty()) {
      continue;
    }

    VLOG(9) << completed.size() << " aio[s] compelted : batch size = "
            << GetTotalSize(completed, completed.size());

    for (auto* aio : completed) {
      OnCompleted(aio);
    }
  }
}

void AioQueueImpl::OnError(AioClosure* aio, Status status) {
  // TODO: CHECK_LT(phase, Phase::kExecuteIO);

  aio->status() = status;
  RunClosure(aio);
}

void AioQueueImpl::OnCompleted(AioClosure* aio) {
  // NOTE: The status of completed aio is set in the io_ring.
  auto phase = aio->timer.GetPhase();

  CHECK(phase == Phase::kExecuteIO) << absl::StrFormat(
      "%s it not on expected phase: got(%s) != expect(%s)", aio->ToString(),
      StrPhase(phase), StrPhase(Phase::kExecuteIO));

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
  LogIt(absl::StrFormat("%s: %s%s <%.6lf>", description, status.ToString(),
                        timer.ToString(), timer.UElapsed() / 1e6));

  infight_throttle_.Decrement(1);
}

}  // namespace cache
}  // namespace dingofs
