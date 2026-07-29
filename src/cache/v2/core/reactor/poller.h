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

#ifndef DINGOFS_CACHE_CORE_REACTOR_POLLER_H_
#define DINGOFS_CACHE_CORE_REACTOR_POLLER_H_

#include <cstddef>
#include <vector>

namespace dingofs {
namespace cache {

// Extension point for work sources polled by the reactor loop (SMP queues,
// RDMA CQs, the IO ring, ...). Contract mirrors seastar's pollfn:
//  - Poll: do pending work, return true if any work was done.
//  - PurePoll: check only (used while idle-spinning), no side effects
//    required beyond detection.
//  - TryEnterInterruptMode: arm a wakeup source so the reactor may block in
//    the kernel; return false to veto sleeping (work appeared / cannot arm).
//  - ExitInterruptMode: disarm after waking.
//  - Flush: hand anything prepared-but-not-submitted to the kernel. Called
//    mid-batch, so a task's submissions need not wait for the batch to end.
class Poller {
 public:
  virtual ~Poller() = default;
  virtual bool Poll() = 0;
  virtual bool PurePoll() { return Poll(); }
  virtual bool TryEnterInterruptMode() { return true; }
  virtual void ExitInterruptMode() {}
  virtual void Flush() {}
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_CORE_REACTOR_POLLER_H_
