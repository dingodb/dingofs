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

#ifndef DINGOFS_BLOCKCACHE_UTILS_CONTAINERS_ADMISSION_QUEUE_H_
#define DINGOFS_BLOCKCACHE_UTILS_CONTAINERS_ADMISSION_QUEUE_H_

#include <cstdint>

#include "blockcache/utils/containers/park_queue.h"

namespace dingofs {
namespace blockcache {

template <typename Op>
class AdmissionQueue {
 public:
  explicit AdmissionQueue(uint32_t limit = 128) : limit_(limit) {}

  void Admit(Op* op) {
    if (inflight_ < limit_) {
      ++inflight_;
      op->Submit();
    } else {
      parked_.Push(op);
      ++total_parked_;
    }
  }

  void OnComplete() {
    Op* next = parked_.Pop();
    if (next != nullptr) {
      next->Submit();
    } else {
      --inflight_;
    }
  }

  uint32_t limit() const { return limit_; }
  uint32_t inflight() const { return inflight_; }
  uint32_t parked() const { return parked_.size(); }
  uint64_t total_parked() const { return total_parked_; }

 private:
  uint32_t limit_;
  uint32_t inflight_ = 0;
  uint64_t total_parked_ = 0;
  ParkQueue<Op> parked_;
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_UTILS_CONTAINERS_ADMISSION_QUEUE_H_
