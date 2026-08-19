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

#include "blockcache/core/reactor/reactor.h"

#include <glog/logging.h>

#include "blockcache/core/memory/shard_allocator.h"

namespace dingofs {
namespace blockcache {

Reactor::Reactor(unsigned shard_id, const ReactorOption& option)
    : shard_id_(shard_id),
      dispatcher_(option.dispatcher),
      timers_(dispatcher_),
      idle_(option.poll_mode) {
  CHECK(tls_reactor == nullptr) << "one reactor per thread";
  tls_reactor = this;
}

Reactor::~Reactor() { tls_reactor = nullptr; }

// task -> submit event -> poll event -> idle spin -> sleep
void Reactor::Run() {
  constexpr uint32_t kFlushEvery = 16;
  Dispatcher::RunScope running(dispatcher_);  // preempt signal + quota tick
  for (;;) {
    tasks_.RunBatch([this] { pollers_.Flush(); }, kFlushEvery);
    if (stopped_) {
      break;
    }

    if (PollOnce() || !tasks_.empty()) {
      stats_.idle_ns += idle_.EndIdle();  // work: the idle streak (if any) ends
      continue;
    }

    if (idle_.Spin()) {
      TrySleep();
    }
  }

  tasks_.Drain();
  stats_.idle_ns += idle_.EndIdle();
  stopped_ = false;
}

// Always posted via the inbox, so it runs on this thread: stopped_ stays plain.
[[gnu::noinline, gnu::cold]] void Reactor::Shutdown() {
  DCHECK(tls_reactor == this) << "Shutdown off the owning shard thread";
  stopped_ = true;
}

void Reactor::Wakeup() {
  if (bell_.ClaimWakeup()) {
    dispatcher_.Notify();
  }
}

bool Reactor::PollOnce() {
  bool work = dispatcher_.ProcessEvents() > 0;
  work |= memory::DrainCrossShardFree() > 0;
  work |= pollers_.Poll();
  return work;
}

bool Reactor::AnyWork() {
  return stopped_ || dispatcher_.HasReadyEvent() || pollers_.PurePoll() ||
         !tasks_.empty();
}

void Reactor::TrySleep() {
  bell_.Arm();
  if (AnyWork() || !pollers_.TryEnterInterruptMode()) {
    bell_.Disarm();
    return;
  }

  ++stats_.sleeps;
  dispatcher_.WaitForEvent();  // blocks until an event lands (no quota tick)
  bell_.Disarm();
  pollers_.ExitInterruptMode();
}

}  // namespace blockcache
}  // namespace dingofs
