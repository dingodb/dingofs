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

#ifndef DINGOFS_CACHE_V2_CORE_REACTOR_PREEMPT_H_
#define DINGOFS_CACHE_V2_CORE_REACTOR_PREEMPT_H_

#include <cstdint>

namespace dingofs {
namespace cache {
namespace v2 {

inline constexpr uint32_t kNeverPreempt = 0;

struct PreemptMonitor {
  const uint32_t* head = &kNeverPreempt;
  const uint32_t* tail = &kNeverPreempt;
  const uint32_t* flags = &kNeverPreempt;
  uint32_t flag_mask = 0;
};

constinit inline thread_local PreemptMonitor
    __attribute__((tls_model("initial-exec"))) tls_preempt_monitor{};

[[gnu::always_inline]] inline bool NeedPreempt() {
  const PreemptMonitor& m = tls_preempt_monitor;
  return __builtin_expect(
      __atomic_load_n(m.head, __ATOMIC_RELAXED) !=
              __atomic_load_n(m.tail, __ATOMIC_RELAXED) ||
          (__atomic_load_n(m.flags, __ATOMIC_RELAXED) & m.flag_mask) != 0,
      false);
}

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_REACTOR_PREEMPT_H_
