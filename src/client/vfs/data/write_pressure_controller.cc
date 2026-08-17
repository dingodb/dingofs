/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#include "client/vfs/data/write_pressure_controller.h"

#include <glog/logging.h>

#include <algorithm>
#include <cstdint>

#include "client/vfs/data/writer_table.h"
#include "utils/executor/executor.h"

namespace dingofs {
namespace client {
namespace vfs {

WritePressureController::WritePressureController(WriterTable* writer_table,
                                                 Executor* executor)
    : writer_table_(CHECK_NOTNULL(writer_table)),
      executor_(CHECK_NOTNULL(executor)),
      event_num_("vfs_write_pressure_event_num"),
      coalesced_event_num_("vfs_write_pressure_coalesced_event_num"),
      round_num_("vfs_write_pressure_round_num"),
      round_failure_num_("vfs_write_pressure_round_failure_num"),
      submit_failure_num_("vfs_write_pressure_submit_failure_num") {}

WritePressureController::~WritePressureController() { StopAndDrain(); }

void WritePressureController::OnWritePressure() {
  bool submit = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopped_) return;
    ++event_epoch_;
    event_num_ << 1;
    if (running_) {
      pending_ = true;
      coalesced_event_num_ << 1;
      return;
    }
    running_ = true;
    pending_ = false;
    submit = true;
  }
  if (submit) SubmitRound(/*attempt=*/0);
}

void WritePressureController::SubmitRound(uint32_t attempt) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopped_) {
      running_ = false;
      pending_ = false;
      cv_.notify_all();
      return;
    }
  }

  if (executor_->Execute([this] { RunRound(); })) return;

  submit_failure_num_ << 1;
  if (attempt < kMaxSubmitRetries) {
    const int delay_ms = kInitialRetryDelayMs << attempt;
    if (executor_->Schedule([this, attempt] { SubmitRound(attempt + 1); },
                            delay_ms)) {
      return;
    }
    submit_failure_num_ << 1;
  }
  FinishSubmitFailure();
}

void WritePressureController::FinishSubmitFailure() {
  std::lock_guard<std::mutex> lock(mutex_);
  running_ = false;
  pending_ = false;
  cv_.notify_all();
}

void WritePressureController::RunRound() {
  uint64_t event_snapshot = 0;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopped_) {
      running_ = false;
      pending_ = false;
      cv_.notify_all();
      return;
    }
    event_snapshot = event_epoch_;
    pending_ = false;
    round_num_ << 1;
  }

  writer_table_->FlushDirtyAsync([this, event_snapshot](Status status) mutable {
    OnRoundDone(event_snapshot, std::move(status));
  });
}

void WritePressureController::OnRoundDone(uint64_t event_snapshot,
                                          Status status) {
  if (!status.ok()) {
    round_failure_num_ << 1;
    LOG(WARNING) << "write pressure flush round failed: " << status.ToString();
  }

  bool run_again = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopped_) {
      running_ = false;
      pending_ = false;
      cv_.notify_all();
      return;
    }

    if (pending_ || event_epoch_ != event_snapshot) {
      pending_ = false;
      run_again = true;
    } else {
      running_ = false;
      cv_.notify_all();
    }
  }
  if (run_again) SubmitRound(/*attempt=*/0);
}

void WritePressureController::StopAndDrain() {
  std::unique_lock<std::mutex> lock(mutex_);
  if (!stopped_) {
    stopped_ = true;
    pending_ = false;
  }
  cv_.wait(lock, [this] { return !running_; });
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
