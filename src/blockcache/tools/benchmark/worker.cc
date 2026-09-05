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

#include "blockcache/tools/benchmark/worker.h"

#include <glog/logging.h>

#include <chrono>
#include <cstring>
#include <thread>

#include "blockcache/tools/benchmark/option.h"

namespace dingofs {
namespace blockcache {

Worker::Worker(uint64_t idx, char* buffer, TaskFactorySPtr factory,
               Collector::Slot* stats)
    : idx_(idx),
      factory_(factory),
      stats_(stats),
      slots_(FLAGS_iodepth),
      window_(FLAGS_iodepth),
      done_(1) {
  std::memset(buffer, 'D', static_cast<size_t>(FLAGS_iodepth) * FLAGS_blksize);
  for (uint32_t i = 0; i < FLAGS_iodepth; i++) {
    slots_[i].data = buffer + (static_cast<size_t>(i) * FLAGS_blksize);
    free_slots_.push_back(&slots_[i]);
  }
}

void Worker::Start() {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(FLAGS_runtime);

  BlockKeyIterator iter(idx_, FLAGS_fsid, FLAGS_blksize, FLAGS_blocks);
  const bool sweep = (FLAGS_op == "get");
  const uint64_t begin = sweep ? FLAGS_offset : 0;
  const uint64_t step = sweep ? FLAGS_length : FLAGS_blksize;
  bool stop = false;
  do {
    for (iter.SeekToFirst(); iter.Valid() && !stop; iter.Next()) {
      const auto key = iter.Key();
      for (uint64_t offset = begin; offset < FLAGS_blksize; offset += step) {
        if (FLAGS_time_based && std::chrono::steady_clock::now() >= deadline) {
          stop = true;
          break;
        }
        SubmitOne(key, offset, std::min(step, FLAGS_blksize - offset));
      }
    }
  } while (FLAGS_time_based && !stop);

  Drain();
  done_.signal();
}

void Worker::Shutdown() { done_.wait(); }

void Worker::SubmitOne(BlockHandle key, uint64_t offset, uint64_t length) {
  window_.acquire();
  Slot* slot = PopSlot();
  slot->key = key;
  slot->offset = offset;
  slot->length = length;

  for (;;) {
    slot->t0 = std::chrono::steady_clock::now();
    bool submitted = factory_->SubmitTask(
        slot, [this, slot](Status status) { OnComplete(slot, status); });
    if (submitted) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }

  VLOG(9) << "Submit task (key=" << key.Filename() << " offset=" << offset
          << " length=" << length << ").";
}

void Worker::OnComplete(Slot* slot, Status status) {
  const uint64_t latency_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - slot->t0)
          .count();

  if (!status.ok()) {
    LOG_EVERY_N(ERROR, 1000) << "Task on block (key=" << slot->key.Filename()
                             << ") failed: " << status.ToString();
  }

  stats_->Add(factory_->BytesPerOp(slot), latency_ns);

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

}  // namespace blockcache
}  // namespace dingofs
