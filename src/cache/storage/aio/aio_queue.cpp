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
#include <bthread/bthread.h>
#include <glog/logging.h>

#include <cstring>
#include <ctime>
#include <memory>
#include <thread>

#include "base/string/string.h"
#include "cache/storage/aio/aio.h"
#include "cache/utils/access_log.h"
#include "cache/utils/helper.h"
#include "cache/utils/phase_timer.h"

namespace dingofs {
namespace cache {
namespace storage {

using dingofs::cache::utils::LogIt;
using dingofs::cache::utils::Phase;

AioQueueImpl::AioQueueImpl(std::shared_ptr<IORing> io_ring)
    : running_(false),
      ioring_(io_ring),
      prep_io_queue_id_({0}),
      queued_(io_ring->GetIODepth()) {}

Status AioQueueImpl::Init() {
  if (running_.exchange(true)) {  // already running
    return Status::OK();
  }

  Status status = ioring_->Init();
  if (!status.ok()) {
    LOG(ERROR) << "Init io ring failed: status = " << status.ToString();
    return status;
  }

  bthread::ExecutionQueueOptions queue_options;
  queue_options.use_pthread = true;
  int rc = bthread::execution_queue_start(&prep_io_queue_id_, &queue_options,
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
  }
  return Status::OK();
}

void AioQueueImpl::EnterPhase(AioClosure* aio, Phase phase) {
  aio->NextPhase(phase);
}

void AioQueueImpl::BatchEnterPhase(AioClosure* aios[], int n, Phase phase) {
  for (int i = 0; i < n; i++) {
    aios[i]->NextPhase(phase);
  }
}

void AioQueueImpl::Submit(AioClosure* aio) {
  EnterPhase(aio, Phase::kQueued);
  queued_.AddOne();

  EnterPhase(aio, Phase::kCheckIO);
  CheckIO(aio);
}

void AioQueueImpl::CheckIO(AioClosure* aio) {
  if (aio->Fd() <= 0 || aio->Length() < 0) {
    RunClosure(aio);
    return;
  }

  EnterPhase(aio, Phase::kEnqueue);
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
      queue->RunClosure(aio);
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
      RunClosure(aios[i]);
    }
  }
}

void AioQueueImpl::BackgroundWait() {
  Aios completed;
  while (running_.load(std::memory_order_relaxed)) {
    Status status = ioring_->WaitIO(1000, &completed);
    if (!status.ok()) {
      continue;
    }

    for (auto* aio : completed) {
      RunClosure(aio);
    }
  }
}

void AioQueueImpl::RunClosure(AioClosure* aio) {
  auto timer = aio->Timer();
  auto status = GetStatus(aio);
  if (!status.ok()) {
    LOG(ERROR) << "Aio error raised for " << aio->ToString() << " in "
               << StrPhase(timer.CurrentPhase())
               << " phase: " << status.ToString();
  }

  // access log
  LogIt(absl::StrFormat("%s: %s%s <.6lf>", aio->ToString(), status.ToString(),
                        timer.ToString()));

  aio->Run();  // run closure
  queued_.SubOne();
}

Status AioQueueImpl::GetStatus(AioClosure* aio) {
  Status status;
  switch (aio->GetPhase()) {
    case Phase::kCheckIO:
      status = Status::InvalidParam("invalid aio param");
      break;

    case Phase::kPrepareIO:
      status = Status::Internal("prepare aio failed");
      break;

    case Phase::kSubmitIO:
      status = Status::Internal("submit aio failed");
      break;

    case Phase::kExecuteIO:
      // status will set by io_ring
      break;

    default:
      status = Status::Unknown("unknown aio error");
  }
  return status;
}

}  // namespace storage
}  // namespace cache
}  // namespace dingofs
