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

#ifndef DINGOFS_CACHE_CORE_UTILS_CONTAINERS_MPSC_QUEUE_H_
#define DINGOFS_CACHE_CORE_UTILS_CONTAINERS_MPSC_QUEUE_H_

#include <atomic>

namespace dingofs {
namespace cache {

struct MpscNode {
  MpscNode* next = nullptr;
};

class MpscQueue {
 public:
  MpscQueue() = default;

  MpscQueue(const MpscQueue&) = delete;
  MpscQueue& operator=(const MpscQueue&) = delete;

  void Push(MpscNode* node) {
    MpscNode* head = head_.load(std::memory_order_relaxed);
    do {
      node->next = head;
    } while (!head_.compare_exchange_weak(head, node, std::memory_order_release,
                                          std::memory_order_relaxed));
  }

  MpscNode* Drain() {
    MpscNode* node = head_.exchange(nullptr, std::memory_order_acquire);
    MpscNode* ordered = nullptr;
    while (node != nullptr) {
      MpscNode* next = node->next;
      node->next = ordered;
      ordered = node;
      node = next;
    }
    return ordered;
  }

  bool Empty() const {
    return head_.load(std::memory_order_relaxed) == nullptr;
  }

 private:
  std::atomic<MpscNode*> head_{nullptr};
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_CORE_UTILS_CONTAINERS_MPSC_QUEUE_H_
