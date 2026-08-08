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

#ifndef DINGOFS_CACHE_V2_UTILS_CONTAINERS_PARK_QUEUE_H_
#define DINGOFS_CACHE_V2_UTILS_CONTAINERS_PARK_QUEUE_H_

#include <cstdint>

namespace dingofs {
namespace cache {
namespace v2 {

template <typename Node>
class ParkQueue {
 public:
  void Push(Node* node) {
    node->park_next = nullptr;
    if (tail_ != nullptr) {
      tail_->park_next = node;
    } else {
      head_ = node;
    }
    tail_ = node;
    ++size_;
  }

  Node* Pop() {
    Node* node = head_;
    if (node == nullptr) {
      return nullptr;
    }

    head_ = node->park_next;
    if (head_ == nullptr) {
      tail_ = nullptr;
    }
    --size_;
    node->park_next = nullptr;
    return node;
  }

  Node* TakeAll() {
    Node* node = head_;
    head_ = nullptr;
    tail_ = nullptr;
    size_ = 0;
    return node;
  }

  // `park_next` is read BEFORE f runs: f typically resumes the waiter, which
  // may destroy it.
  template <typename F>
  void TakeAllAnd(F f) {
    Node* node = TakeAll();
    while (node != nullptr) {
      Node* next = node->park_next;
      f(node);
      node = next;
    }
  }

  bool empty() const { return head_ == nullptr; }
  uint32_t size() const { return size_; }

 private:
  Node* head_ = nullptr;
  Node* tail_ = nullptr;
  uint32_t size_ = 0;
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_UTILS_CONTAINERS_PARK_QUEUE_H_
