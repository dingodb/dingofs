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

#include "cache/utils/aio_queue.h"

#include <absl/cleanup/cleanup.h>
#include <bthread/bthread.h>
#include <glog/logging.h>

#include <cstring>
#include <ctime>
#include <memory>
#include <thread>

#include "base/string/string.h"
#include "cache/utils/access_log.h"
#include "cache/utils/aio.h"
#include "cache/utils/helper.h"
#include "cache/utils/phase_timer.h"

namespace dingofs {
namespace cache {
namespace utils {

using dingofs::base::string::StrFormat;

AioQueueImpl::AioQueueImpl(const std::shared_ptr<IoRing>& io_ring)
    : running_(false),
      ioring_(io_ring),
      prep_io_queue_id_({0}),
      queued_(nullptr) {}

Status AioQueueImpl::Init(uint32_t iodepth) {
  if (running_.exchange(true)) {  // already running
    return Status::OK();
  }

  Status status = ioring_->Init(iodepth);
  if (!status.ok()) {
    LOG(ERROR) << "Init io ring failed.";
    return status;
  }

  bthread::ExecutionQueueOptions queue_options;
  queue_options.use_pthread = true;
  int rc = bthread::execution_queue_start(&prep_io_queue_id_, &queue_options,
                                          PrepareIo, this);
  if (rc != 0) {
    LOG(ERROR) << "Start aio prepare execution queue failed: rc = " << rc;
    return Status::Internal("bthread::execution_queue_start() failed");
  }

  bg_thread_ = std::thread(&AioQueueImpl::BackgroundWait, this);
  queued_ = std::make_unique<ThrottleQueue>(iodepth);
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
    bg_thread_.join();
  }
  return Status::OK();
}

void AioQueueImpl::EnterPhase(Aio* aio, Phase phase) {
  aio->timer.NextPhase(phase);
}

void AioQueueImpl::BatchEnterPhase(Aio* aios[], int n, Phase phase) {
  for (int i = 0; i < n; i++) {
    aios[i]->timer.NextPhase(phase);
  }
}

void AioQueueImpl::Submit(Aio* aio) {
  EnterPhase(aio, Phase::kQueued);
  queued_->PushOne();
  EnterPhase(aio, Phase::kCheckIo);
  CheckIo(aio);
}

void AioQueueImpl::CheckIo(Aio* aio) {
  if (aio->fd <= 0 || aio->length < 0) {
    RunClosureInBthread(aio);
    return;
  }

  EnterPhase(aio, Phase::kEnqueue);
  CHECK_EQ(0, bthread::execution_queue_execute(prep_io_queue_id_, aio));
}

int AioQueueImpl::PrepareIo(void* meta, bthread::TaskIterator<Aio*>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  int n = 0;
  static Aio* aios[kSubmitBatchSize];
  AioQueueImpl* queue = static_cast<AioQueueImpl*>(meta);
  auto ioring = queue->ioring_;
  for (; iter; iter++) {
    auto* aio = *iter;
    EnterPhase(aio, Phase::kPrepareIo);
    Status status = ioring->PrepareIo(aio);
    if (!status.ok()) {
      queue->RunClosureInBthread(aio);
      continue;
    }

    aios[n++] = aio;
    if (n == kSubmitBatchSize) {
      BatchEnterPhase(aios, n, Phase::kSubmitIo);
      queue->BatchSubmitIo(aios, n);
      n = 0;
    }
  }

  if (n > 0) {
    queue->BatchSubmitIo(aios, n);
  }
  return 0;
}

void AioQueueImpl::BatchSubmitIo(Aio* aios[], int n) {
  Status status = ioring_->SubmitIo();
  for (int i = 0; i < n; i++) {
    if (!status.ok()) {
      EnterPhase(aios[i], Phase::kExecuteIo);
    } else {
      RunClosureInBthread(aios[i]);
    }
  }
}

void AioQueueImpl::BackgroundWait() {
  std::vector<Aio*> completed;
  while (running_.load(std::memory_order_relaxed)) {
    Status status = ioring_->WaitIo(1000, &completed);
    if (!status.ok()) {
      continue;
    }

    for (auto* aio : completed) {
      RunClosureInBthread(aio);
    }
  }
}

void AioQueueImpl::RunClosureInBthread(Aio* aio) {
  bthread_t tid;
  bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
  BthreadArg* arg = new BthreadArg(this, aio);
  int rc = bthread_start_background(&tid, &attr, RunClosure, arg);
  if (rc != 0) {
    LOG(ERROR) << "Fail to start bthread to run aio closure.";
    RunClosure(arg);
    delete arg;
  }
}

void* AioQueueImpl::RunClosure(void* arg) {
  BthreadArg* bthread_arg = (BthreadArg*)arg;
  AioQueueImpl* queue = bthread_arg->queue;
  Aio* aio = bthread_arg->aio;

  // NOTE: aio struct maybe freed once closure executed,
  // so we should copy someone we needed.
  std::string str_aio = StrAio(aio);
  auto timer = aio->timer;
  Phase curr_phase = timer.CurrentPhase();
  Status status = GetStatus(aio);

  // copy memory
  if (aio->aio_type == AioType::kRead) {
    timer.NextPhase(Phase::kMemcpy);
    queue->ioring_->PostIo(aio);
  }

  // run closure
  if (aio->done != nullptr) {
    timer.NextPhase(Phase::kRunClosure);
    aio->done->SetStatus(status);
    aio->done->Run();
  }

  // log
  if (!status.ok()) {
    LOG(ERROR) << "Aio error raised for " << StrAio(aio) << " in "
               << StrPhase(curr_phase) << " phase: " << status.ToString();
  }
  LogIt(StrFormat("%s: %s%s <%.6lf>", str_aio, status.ToString(),
                  timer.ToString(), timer.TotalUElapsed() / 1e6));

  delete bthread_arg;
  queue->queued_->PopOne();
  return nullptr;
}

Status AioQueueImpl::GetStatus(Aio* aio) {
  Status status;
  switch (aio->timer.CurrentPhase()) {
    case Phase::kCheckIo:
      status = Status::InvalidParam("invalid aio param");
      break;

    case Phase::kPrepareIo:
      status = Status::Internal("prepare aio failed");
      break;

    case Phase::kSubmitIo:
      status = Status::Internal("submit aio failed");
      break;

    case Phase::kExecuteIo:
      if (aio->retcode == 0) {
        status = Status::OK();
      } else {
        status = Status::IoError("aio io error");
      }
      break;

    default:
      status = Status::Unknown("unknown aio error");
  }
  return status;
}

}  // namespace utils
}  // namespace cache
}  // namespace dingofs
