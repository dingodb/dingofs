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

#ifndef DINGOFS_BLOCKCACHE_CORE_REACTOR_POLLER_H_
#define DINGOFS_BLOCKCACHE_CORE_REACTOR_POLLER_H_

#include <cstddef>
#include <vector>

namespace dingofs {
namespace blockcache {

// Work source polled by the reactor loop (SMP queues, RDMA CQs, IO ring).
class Poller {
 public:
  virtual ~Poller() = default;

  virtual bool Poll() = 0;  // do pending work; true if any was done
  virtual bool PurePoll() { return Poll(); }  // detection only, while spinning

  virtual bool TryEnterInterruptMode() { return true; }  // false vetoes sleep
  virtual void ExitInterruptMode() {}

  virtual void Flush() {}  // submit prepared work; called mid task batch
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_CORE_REACTOR_POLLER_H_
