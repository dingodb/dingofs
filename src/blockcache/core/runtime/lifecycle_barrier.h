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

#ifndef DINGOFS_BLOCKCACHE_CORE_RUNTIME_LIFECYCLE_BARRIER_H_
#define DINGOFS_BLOCKCACHE_CORE_RUNTIME_LIFECYCLE_BARRIER_H_

#include <barrier>
#include <latch>

namespace dingofs {
namespace blockcache {

class LifecycleBarrier {
 public:
  explicit LifecycleBarrier(unsigned shard_count)
      : all_started_(shard_count + 1),
        all_stopped_(shard_count),
        stop_issued_(1) {}

  LifecycleBarrier(const LifecycleBarrier&) = delete;
  LifecycleBarrier& operator=(const LifecycleBarrier&) = delete;

  void WaitAllStarted() { all_started_.arrive_and_wait(); }
  void WaitAllStopped() { all_stopped_.arrive_and_wait(); }
  void IssueStop() { stop_issued_.count_down(); }
  void WaitStopIssued() { stop_issued_.wait(); }

 private:
  std::barrier<> all_started_;
  std::barrier<> all_stopped_;
  std::latch stop_issued_;
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_CORE_RUNTIME_LIFECYCLE_BARRIER_H_
