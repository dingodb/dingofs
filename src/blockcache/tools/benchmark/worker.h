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

#ifndef DINGOFS_BLOCKCACHE_TOOLS_BENCHMARK_WORKER_H_
#define DINGOFS_BLOCKCACHE_TOOLS_BENCHMARK_WORKER_H_

#include <bthread/countdown_event.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <semaphore>
#include <vector>

#include "blockcache/tools/benchmark/collector.h"
#include "blockcache/tools/benchmark/factory.h"

namespace dingofs {
namespace blockcache {

class Worker {
 public:
  Worker(uint64_t idx, char* buffer, TaskFactorySPtr factory,
         CollectorSPtr collector);
  Worker(const Worker&) = delete;
  Worker& operator=(const Worker&) = delete;

  void Start();
  void Shutdown();

 private:
  void SubmitOne(BlockHandle key, uint64_t offset, uint64_t length);
  void OnComplete(Slot* slot, Status status);
  Slot* PopSlot();
  void PushSlot(Slot* slot);
  void Drain();

  uint64_t idx_;
  TaskFactorySPtr factory_;
  CollectorSPtr collector_;
  std::vector<Slot> slots_;
  std::mutex mutex_;
  std::vector<Slot*> free_slots_;
  std::counting_semaphore<> window_;
  bthread::CountdownEvent done_;
};

using WorkerUPtr = std::unique_ptr<Worker>;

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_TOOLS_BENCHMARK_WORKER_H_
