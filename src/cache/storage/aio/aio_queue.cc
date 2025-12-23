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

#include "cache/common/macro.h"
#include "cache/storage/aio/aio.h"
#include "cache/utils/infight_throttle.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

DEFINE_uint32(ioring_iodepth, 128, "Aio queue maximum iodepth");

const std::string kModule = "aio";

AioQueueImpl::AioQueueImpl(std::shared_ptr<IORing> io_ring)
    : running_(false),
      infights_(std::make_unique<InflightThrottle>(FLAGS_ioring_iodepth)),
      ioring_(io_ring),
      prep_io_queue_id_({0}) {}

Status AioQueueImpl::Start() {
  CHECK_NOTNULL(ioring_);
  CHECK_NOTNULL(infights_);

  if (running_) {
    return Status::OK();
  }

  LOG(INFO) << "Aio queue is starting...";

  bthread::ExecutionQueueOptions options;
  options.use_pthread = true;
  int rc = bthread::execution_queue_start(&prep_io_queue_id_, &options,
                                          PrepareIO, this);
  if (rc != 0) {
    LOG(ERROR) << "Start execution queue failed: rc = " << rc;
    return Status::Internal("start execution queue failed");
  }

  bg_wait_thread_ = std::thread(&AioQueueImpl::BackgroundWait, this);

  running_ = true;

  LOG(INFO) << "Aio queue is up: iodepth = " << FLAGS_ioring_iodepth;

  CHECK_RUNNING("Aio queue");
  return Status::OK();
}

Status AioQueueImpl::Shutdown() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }

  LOG(INFO) << "Aio queue is shutting down...";

  if (bthread::execution_queue_stop(prep_io_queue_id_) != 0) {
    LOG(ERROR) << "Stop execution queue failed.";
    return Status::Internal("stop execution queue failed");
  } else if (bthread::execution_queue_join(prep_io_queue_id_) != 0) {
    LOG(ERROR) << "Join execution queue failed.";
    return Status::Internal("join execution queue failed");
  }

  bg_wait_thread_.join();

  LOG(INFO) << "Aio queue is down.";

  CHECK_DOWN("Aio queue");
  return Status::OK();
}

void AioQueueImpl::Submit(Aio* aio) {
  CHECK_RUNNING("Aio queue");

  VLOG(9) << "Aio submitted: aio = " << aio->ToString();

  aio->timer.Start();

  // TODO: pls consider all waiting inflight bthread wakeup at the same time,
  // it will leads all bthread fight for locks. any performance skew?
  NextStep(aio, "throttle");
  infights_->Increment(1);

  NextStep(aio, "check");
  CheckIO(aio);
}

void AioQueueImpl::CheckIO(Aio* aio) {
  if (aio->fd <= 0 || aio->length < 0) {
    OnError(aio, Status::Internal("invalid aio param"));
    return;
  }

  NextStep(aio, "enqueue");
  CHECK_EQ(0, bthread::execution_queue_execute(prep_io_queue_id_, aio));

  // NextStep(aio, "wait");
}

int AioQueueImpl::PrepareIO(void* meta, bthread::TaskIterator<Aio*>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  std::vector<Aio*> prep_aios;
  prep_aios.reserve(kSubmitBatchSize);
  AioQueueImpl* self = static_cast<AioQueueImpl*>(meta);
  auto ioring = self->ioring_;
  for (; iter; iter++) {
    auto* aio = *iter;
    NextStep(aio, "prepare");
    Status status = ioring->PrepareIO(aio);
    if (!status.ok()) {
      self->OnError(aio, status);
      continue;
    }

    prep_aios.emplace_back(aio);
    if (prep_aios.size() == kSubmitBatchSize) {
      BatchNextStep(prep_aios, "execute");
      self->BatchSubmitIO(prep_aios);
      prep_aios.clear();
    }
  }

  if (!prep_aios.empty()) {
    BatchNextStep(prep_aios, "execute");
    self->BatchSubmitIO(prep_aios);
  }
  return 0;
}

void AioQueueImpl::BatchSubmitIO(const std::vector<Aio*>& aios) {
  Status status = ioring_->SubmitIO();
  if (!status.ok()) {
    for (auto* aio : aios) {
      OnError(aio, status);
    }
    return;
  }

  VLOG(9) << aios.size()
          << " aio[s] submitted: total length = " << GetTotalLength(aios);
}

void AioQueueImpl::BackgroundWait() {
  std::vector<Aio*> completed_aios;
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

void AioQueueImpl::OnError(Aio* aio, Status status) {
  const auto& ctx = aio->ctx;
  CHECK_NE(LastStep(aio), "execute")
      << ctx->StrTraceId() << " Aio " << aio->ToString()
      << " is not on expected phase: got(" << LastStep(aio)
      << ") != expect(execute)";

  LOG_CTX(ERROR) << "Aio encountered an error in " << LastStep(aio)
                 << " step: aio = " << aio->ToString()
                 << ", status = " << status.ToString();

  aio->status() = status;
  RunClosure(aio);
}

void AioQueueImpl::OnCompleted(Aio* aio) {
  const auto& ctx = aio->ctx;

  CHECK_EQ(LastStep(aio), "execute")
      << ctx->StrTraceId() << " Aio " << aio->ToString()
      << " is not on expected phase: got(" << LastStep(aio)
      << ") != expect(execute)";

  if (!aio->status().ok()) {
    LOG_CTX(ERROR) << "Aio failed: aio = " << aio->ToString()
                   << ", status = " << aio->status().ToString();
  }

  RunClosure(aio);
}

// NOTE: The aio will been freed once closure runned
void AioQueueImpl::RunClosure(Aio* aio) {
  auto status = aio->status();
  auto timer = aio->timer;
  TraceLogGuard log(aio->ctx, status, timer, kModule, "%s", aio->ToString());

  NextStep(aio, "closure");
  aio->Run();

  infights_->Decrement(1);

  timer.Stop();
}

std::string AioQueueImpl::LastStep(Aio* aio) { return aio->timer.LastStep(); }

void AioQueueImpl::NextStep(Aio* aio, const std::string& step_name) {
  auto& timer = aio->timer;
  NEXT_STEP(step_name);
}

void AioQueueImpl::BatchNextStep(const std::vector<Aio*>& aios,
                                 const std::string& step_name) {
  for (auto* aio : aios) {
    NextStep(aio, step_name);
  }
}

uint64_t AioQueueImpl::GetTotalLength(const std::vector<Aio*>& aios) {
  uint64_t total_length = 0;
  for (auto* aio : aios) {
    total_length += aio->length;
  }
  return total_length;
}

}  // namespace cache
}  // namespace dingofs
