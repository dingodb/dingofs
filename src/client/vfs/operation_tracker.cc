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

#include "client/vfs/operation_tracker.h"

#include <chrono>

namespace dingofs {
namespace client {
namespace {

std::atomic<uint64_t> next_tracker_thread_index{0};

}  // namespace

uint64_t OperationTracker::AllocateThreadIndex() {
  const auto index =
      next_tracker_thread_index.fetch_add(1, std::memory_order_seq_cst);
  CHECK_NE(index, std::numeric_limits<uint64_t>::max())
      << "Session operation thread index overflow";
  return index;
}

void OperationTracker::OpenOnce() {
  CHECK(!opened_) << "Operation tracker cannot reopen";
  opened_ = true;
  epoch_.store(2, std::memory_order_seq_cst);
}

bool OperationTracker::IsDrained() const {
  for (const auto& counter : counts_) {
    if (counter.value.load(std::memory_order_seq_cst) != 0) return false;
  }
  return true;
}

void OperationTracker::WaitForDrain() {
  CHECK_NE(epoch_.load(std::memory_order_seq_cst) & 1, 0)
      << "Close the operation tracker before draining";
  std::unique_lock<std::mutex> lock(drain_mutex_);
  // Accepted operations can only disappear after Close. Late tentative
  // increments need not be frozen: their second epoch check must reject them.
  while (!IsDrained()) {
    TEST_SYNC_POINT_CALLBACK("OperationTracker::BeforeWait", this);
    if (drain_cv_.wait_for(lock, std::chrono::seconds(30)) ==
            std::cv_status::timeout &&
        !IsDrained()) {
      unsigned busy_slots = 0;
      for (const auto& counter : counts_) {
        if (counter.value.load(std::memory_order_seq_cst) != 0) ++busy_slots;
      }
      LOG(ERROR) << "VFS Stop still waiting for public operations to drain in "
                 << busy_slots << " operation tracker slot(s)";
    }
  }
}

}  // namespace client
}  // namespace dingofs
