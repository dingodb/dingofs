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

#ifndef DINGOFS_CACHE_V2_UTILS_CONTAINERS_MPSC_QUEUE_H_
#define DINGOFS_CACHE_V2_UTILS_CONTAINERS_MPSC_QUEUE_H_

#include <atomic>

namespace dingofs {
namespace cache {
namespace v2 {

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

  MpscNode* TakeAll() {
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

  // `next` is read BEFORE f runs: the callback owns the node and may free it.
  template <typename F>
  unsigned ConsumeAll(F f) {
    unsigned n = 0;
    MpscNode* node = TakeAll();
    while (node != nullptr) {
      MpscNode* next = node->next;
      f(node);
      node = next;
      ++n;
    }
    return n;
  }

  bool empty() const {
    return head_.load(std::memory_order_relaxed) == nullptr;
  }

 private:
  std::atomic<MpscNode*> head_{nullptr};
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_UTILS_CONTAINERS_MPSC_QUEUE_H_
