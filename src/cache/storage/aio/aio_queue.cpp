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
#include <glog/logging.h>

#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include "cache/common/const.h"
#include "cache/common/macro.h"
#include "cache/storage/aio/aio.h"
#include "cache/utils/infight_throttle.h"
#include "cache/utils/step_timer.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

const std::string kModule = kAioModule;

AioQueueImpl::AioQueueImpl(std::shared_ptr<IORing> io_ring)
    : running_(false),
      ioring_(io_ring),
      infight_throttle_(
          std::make_unique<InflightThrottle>(ioring_->GetIODepth())),
      prep_io_queue_id_({0}),
      prep_aios_(kSubmitBatchSize) {}

Status AioQueueImpl::Start() {
  CHECK_NOTNULL(ioring_);
  CHECK_NOTNULL(infight_throttle_);

  if (running_) {
    return Status::OK();
  }

  LOG_INFO("Aio queue is starting...");

  Status status = ioring_->Start();
  if (!status.ok()) {
    LOG_ERROR("Start io ring failed: %s", status.ToString());
    return status;
  }

  bthread::ExecutionQueueOptions options;
  options.use_pthread = true;
  int rc = bthread::execution_queue_start(&prep_io_queue_id_, &options,
                                          PrepareIO, this);
  if (rc != 0) {
    LOG_ERROR("Start execution queue failed: rc = %d", rc);
    return Status::Internal("start execution queue failed");
  }

  bg_wait_thread_ = std::thread(&AioQueueImpl::BackgroundWait, this);

  running_.store(true);

  LOG_INFO("Aio queue is up: iodepth = %d", ioring_->GetIODepth());

  CHECK_RUNNING("Aio queue");
  return Status::OK();
}

Status AioQueueImpl::Shutdown() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }

  LOG_INFO("Aio queue is shutting down...");

  if (bthread::execution_queue_stop(prep_io_queue_id_) != 0) {
    LOG_ERROR("Stop execution queue failed.");
    return Status::Internal("execution_queue_join() failed");
  } else if (bthread::execution_queue_join(prep_io_queue_id_) != 0) {
    LOG_ERROR("Join execution queue failed.");
    return Status::Internal("execution_queue_join() failed");
  }

  bg_wait_thread_.join();

  auto status = ioring_->Shutdown();
  if (!status.ok()) {
    LOG_ERROR("Shutdown io ring failed: %s", status.ToString());
    return status;
  }

  prep_aios_.clear();

  LOG_INFO("Aio queue is shutting down...");

  CHECK_DOWN("Aio queue");
  CHECK_EQ(prep_aios_.size(), 0);
  return Status::OK();
}

void AioQueueImpl::Submit(AioClosure* aio) {
  CHECK_RUNNING("Aio queue");

  NextStep(aio, kWaitThrottle);
  infight_throttle_->Increment(1);

  NextStep(aio, kCheckIo);
  CheckIO(aio);
}

void AioQueueImpl::CheckIO(AioClosure* aio) {
  if (aio->fd <= 0 || aio->length < 0) {
    OnError(aio, Status::Internal("invalid aio param"));
    return;
  }

  NextStep(aio, kEnqueue);
  CHECK_EQ(0, bthread::execution_queue_execute(prep_io_queue_id_, aio));
}

int AioQueueImpl::PrepareIO(void* meta,
                            bthread::TaskIterator<AioClosure*>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  int n = 0;
  AioQueueImpl* queue = static_cast<AioQueueImpl*>(meta);
  std::vector<AioClosure*> aios;

  auto ioring = queue->ioring_;
  for (; iter; iter++) {
    auto* aio = *iter;
    NextStep(aio, kPrepareIO);

    Status status = ioring->PrepareIO(aio);
    if (!status.ok()) {
      queue->OnError(aio, status);
      continue;
    }

    aios.emplace_back(aio);
    if (n == kSubmitBatchSize) {
      BatchNextStep(aios, kExecuteIO);
      queue->BatchSubmitIO(aios, n);
      n = 0;
    }
  }

  if (n > 0) {
    BatchNextStep(aios, kExecuteIO);
    queue->BatchSubmitIO(aios, n);
  }
  return 0;
}

void AioQueueImpl::BatchSubmitIO(const std::vector<AioClosure*>& aios, int n) {
  Status status = ioring_->SubmitIO();
  if (!status.ok()) {
    for (auto* aio : aios) {
      OnError(aio, status);
    }
    return;
  }

  VLOG(9) << n << " aio[s] submitted: total length = " << GetTotalLength(aios);
}

void AioQueueImpl::BackgroundWait() {
  std::vector<AioClosure*> completed_aios;
  while (running_.load(std::memory_order_relaxed)) {
    Status status = ioring_->WaitIO(1000, &completed_aios);
    if (!status.ok() || completed_aios.empty()) {
      continue;
    }

    VLOG(9) << completed_aios.size() << " aio[s] compelted : total length = "
            << GetTotalLength(completed_aios);

    for (auto* aio : completed_aios) {
      OnCompleted(aio);
    }
  }
}

void AioQueueImpl::OnError(AioClosure* aio, Status status) {
  CHECK_NE(GetStep(aio), kExecuteIO)
      << absl::StrFormat("%s it not on expected phase: got(%s) != expect(%s)",
                         aio->ToString(), GetStep(aio), kExecuteIO);

  LOG_ERROR("Aio encountered an error in %s step: aio = %s, status = %s",
            GetStep(aio), aio->ToString(), status.ToString());

  aio->status() = status;
  RunClosure(aio);
}

void AioQueueImpl::OnCompleted(AioClosure* aio) {
  CHECK_EQ(GetStep(aio), kExecuteIO)
      << absl::StrFormat("%s it not on expected phase: got(%s) != expect(%s)",
                         aio->ToString(), GetStep(aio), kExecuteIO);

  if (!aio->status().ok()) {
    LOG_ERROR("Aio failed: aio = %s, status = %s", aio->ToString(),
              aio->status().ToString());
  }

  RunClosure(aio);
}

void AioQueueImpl::RunClosure(AioClosure* aio) {
  auto status = aio->status();
  TracingGuard tracing(aio->ctx, status, kModule, "%s", aio->ToString());

  NextStep(aio, kRunClosure);
  aio->Run();

  infight_throttle_->Decrement(1);
}

std::string AioQueueImpl::GetStep(AioClosure* aio) {
  return aio->timer.GetStep();
}

void AioQueueImpl::NextStep(AioClosure* aio, const std::string& step_name) {
  auto ctx = aio->ctx;
  NEXT_STEP(step_name);
}

void AioQueueImpl::BatchNextStep(const std::vector<AioClosure*>& aios,
                                 const std::string& step_name) {
  for (auto* aio : aios) {
    NextStep(aio, step_name);
  }
}

uint64_t AioQueueImpl::GetTotalLength(const std::vector<AioClosure*>& aios) {
  uint64_t total_length = 0;
  for (auto* aio : aios) {
    total_length += aio->length;
  }
  return total_length;
}

}  // namespace cache
}  // namespace dingofs
