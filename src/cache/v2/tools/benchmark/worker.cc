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

#include "cache/v2/tools/benchmark/worker.h"

#include <glog/logging.h>

#include <chrono>
#include <cstring>
#include <thread>

#include "cache/v2/tools/benchmark/option.h"

namespace dingofs {
namespace cache {
namespace v2 {

Worker::Worker(uint64_t idx, char* buffer, TaskFactorySPtr factory,
               CollectorSPtr collector)
    : idx_(idx),
      factory_(factory),
      collector_(collector),
      slots_(FLAGS_iodepth),
      window_(FLAGS_iodepth),
      done_(1) {
  std::memset(buffer, 'D',
              static_cast<size_t>(FLAGS_iodepth) * FLAGS_blksize);
  for (uint32_t i = 0; i < FLAGS_iodepth; i++) {
    slots_[i].data = buffer + (static_cast<size_t>(i) * FLAGS_blksize);
    free_slots_.push_back(&slots_[i]);
  }
}

void Worker::Start() {
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::seconds(FLAGS_runtime);

  BlockKeyIterator iter(idx_, FLAGS_fsid, FLAGS_blksize, FLAGS_blocks);
  bool stop = false;
  do {
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
      if (FLAGS_time_based && std::chrono::steady_clock::now() >= deadline) {
        stop = true;
        break;
      }
      SubmitOne(iter.Key());
    }
  } while (FLAGS_time_based && !stop);

  Drain();
  done_.signal();
}

void Worker::Shutdown() { done_.wait(); }

void Worker::SubmitOne(const BlockHandle& key) {
  window_.acquire();
  Slot* slot = PopSlot();
  slot->key = key;

  for (;;) {
    slot->t0 = std::chrono::steady_clock::now();
    bool submitted = factory_->SubmitTask(
        slot, [this, slot](Status status) { OnComplete(slot, status); });
    if (submitted) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }

  VLOG(9) << "Submit task (key=" << key.Filename() << ").";
}

void Worker::OnComplete(Slot* slot, Status status) {
  const auto latency_us =
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - slot->t0)
          .count();

  if (!status.ok()) {
    LOG(ERROR) << "Task on block (key=" << slot->key.Filename()
               << ") failed: " << status.ToString();
  }

  const uint64_t bytes = factory_->BytesPerOp();
  collector_->Submit([bytes, latency_us](Stat* stat, Stat* total) {
    stat->Add(bytes, latency_us);
    total->Add(bytes, latency_us);
  });

  PushSlot(slot);
  window_.release();
}

Slot* Worker::PopSlot() {
  std::lock_guard<std::mutex> lock(mutex_);
  Slot* slot = free_slots_.back();
  free_slots_.pop_back();
  return slot;
}

void Worker::PushSlot(Slot* slot) {
  std::lock_guard<std::mutex> lock(mutex_);
  free_slots_.push_back(slot);
}

void Worker::Drain() {
  for (uint32_t i = 0; i < FLAGS_iodepth; i++) {
    window_.acquire();
  }
}

}  // namespace v2
}  // namespace cache
}  // namespace dingofs
