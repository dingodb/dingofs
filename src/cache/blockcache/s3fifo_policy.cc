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

/*
 * Project: DingoFS
 * Created Date: 2026-07-22
 * Author: Jingli Chen (Wine93)
 */

#include "cache/blockcache/s3fifo_policy.h"

#include <absl/hash/hash.h>
#include <glog/logging.h>

#include <algorithm>

namespace dingofs {
namespace cache {

static constexpr size_t kMinGhostEntries = 4096;

S3FIFOPolicy::S3FIFOPolicy(uint64_t capacity_bytes, double small_ratio)
    : small_capacity_(capacity_bytes * small_ratio) {
  QueueInit(&small_);
  QueueInit(&main_);
}

void S3FIFOPolicy::Add(const CacheKey& key, const CacheValue& value) {
  auto [iter, ok] = nodes_.emplace(key.Filename(), Node{value});
  if (!ok) {  // duplicate add: refresh the value in place
    iter->second.value = value;
    return;
  }

  Node* node = &iter->second;
  node->key = &iter->first;
  if (GhostErase(Fingerprint(iter->first))) {
    // evicted-then-refetched: proven worth, readmit straight into main
    ghost_hits_++;
    node->in_small = false;
    QueuePush(&main_, node);
  } else {
    node->in_small = true;
    small_bytes_ += value.size;
    QueuePush(&small_, node);
  }
}

bool S3FIFOPolicy::Touch(const CacheKey& key, CacheValue* value) {
  auto iter = nodes_.find(key.Filename());
  if (iter == nodes_.end()) {
    return false;
  }

  // lazy promotion: only bump the saturating counter, no queue movement
  auto& v = iter->second.value;
  v.atime = iutil::TimeNow();
  if (v.freq < 3) {
    v.freq++;
  }
  *value = v;
  return true;
}

bool S3FIFOPolicy::Exist(const CacheKey& key) const {
  return nodes_.count(key.Filename()) != 0;
}

bool S3FIFOPolicy::Get(const CacheKey& key, CacheValue* value) const {
  auto iter = nodes_.find(key.Filename());
  if (iter == nodes_.end()) {
    return false;
  }
  *value = iter->second.value;
  return true;
}

bool S3FIFOPolicy::Delete(const CacheKey& key, CacheValue* deleted) {
  auto iter = nodes_.find(key.Filename());
  if (iter == nodes_.end()) {
    return false;
  }
  *deleted = iter->second.value;
  RemoveNode(&iter->second, false);
  return true;
}

CacheItems S3FIFOPolicy::Evict(FilterFunc filter) {
  CacheItems evicted;
  // every node is handled at most once per pass (evicted, promoted or
  // reinserted), so an all-skipping filter always terminates
  size_t budget = nodes_.size();
  while (budget-- > 0 && !nodes_.empty()) {
    Node* victim = NextVictim();
    if (victim->value.freq > 0) {
      // survived the queue with hits: promote (small) or reinsert (main)
      victim->value.freq = 0;
      MoveToMain(victim);
      continue;
    }

    auto rc = filter(victim->value);
    if (rc == FilterStatus::kEvictIt) {
      evicted.emplace_back(RemoveNode(victim, victim->in_small));
    } else if (rc == FilterStatus::kSkip) {
      MoveToMain(victim);  // e.g. protected block: give it another round
    } else if (rc == FilterStatus::kFinish) {
      break;
    } else {
      CHECK(false);  // never happen
    }
  }
  return evicted;
}

CacheItems S3FIFOPolicy::Sweep(FilterFunc filter) {
  CacheItems evicted;
  // oldest-to-newest over both queues, no reordering, no ghost bookkeeping
  // (expiry is not a capacity signal)
  for (Node* queue : {&small_, &main_}) {
    for (Node* curr = queue->next; curr != queue;) {
      Node* next = curr->next;
      auto rc = filter(curr->value);
      if (rc == FilterStatus::kEvictIt) {
        evicted.emplace_back(RemoveNode(curr, false));
      } else if (rc == FilterStatus::kSkip) {
        // do nothing
      } else if (rc == FilterStatus::kFinish) {
        return evicted;
      } else {
        CHECK(false);  // never happen
      }
      curr = next;
    }
  }
  return evicted;
}

size_t S3FIFOPolicy::Size() const { return nodes_.size(); }

void S3FIFOPolicy::Clear() {
  nodes_.clear();
  QueueInit(&small_);
  QueueInit(&main_);
  small_bytes_ = 0;
  ghost_hits_ = 0;
  ghost_.clear();
  ghost_fifo_.clear();
}

void S3FIFOPolicy::QueueInit(Node* queue) {
  queue->next = queue;
  queue->prev = queue;
}

void S3FIFOPolicy::QueuePush(Node* queue, Node* node) {
  node->next = queue;
  node->prev = queue->prev;
  node->prev->next = node;
  node->next->prev = node;
}

void S3FIFOPolicy::QueueRemove(Node* node) {
  node->next->prev = node->prev;
  node->prev->next = node->next;
}

bool S3FIFOPolicy::QueueEmpty(const Node* queue) {
  return queue->next == queue;
}

uint64_t S3FIFOPolicy::Fingerprint(const std::string& filename) {
  return absl::Hash<std::string>{}(filename);
}

// prefer the small queue once it exceeds its quota so new blocks keep a
// probationary residence time; otherwise age out of main
S3FIFOPolicy::Node* S3FIFOPolicy::NextVictim() {
  if ((small_bytes_ >= small_capacity_ || QueueEmpty(&main_)) &&
      !QueueEmpty(&small_)) {
    return small_.next;
  }
  return main_.next;
}

void S3FIFOPolicy::MoveToMain(Node* node) {
  QueueRemove(node);
  if (node->in_small) {
    node->in_small = false;
    small_bytes_ -= node->value.size;
  }
  QueuePush(&main_, node);
}

CacheItem S3FIFOPolicy::RemoveNode(Node* node, bool remember_in_ghost) {
  CacheKey key;
  CHECK(key.ParseFromFilename(*node->key)) << "filename = " << *node->key;
  CacheItem item(key, node->value);

  if (remember_in_ghost) {
    GhostInsert(Fingerprint(*node->key));
  }
  if (node->in_small) {
    small_bytes_ -= node->value.size;
  }
  QueueRemove(node);
  // erase by iterator: *node->key is the map entry's own key and must not be
  // passed by reference into erase(const Key&)
  nodes_.erase(nodes_.find(*node->key));
  return item;
}

void S3FIFOPolicy::GhostInsert(uint64_t fingerprint) {
  if (!ghost_.insert(fingerprint).second) {
    return;  // already remembered
  }
  ghost_fifo_.push_back(fingerprint);

  // bounded by the resident entry count: one ghost slot per cached block
  size_t limit = std::max(kMinGhostEntries, nodes_.size());
  while (ghost_fifo_.size() > limit) {
    ghost_.erase(ghost_fifo_.front());
    ghost_fifo_.pop_front();
  }
}

bool S3FIFOPolicy::GhostErase(uint64_t fingerprint) {
  // lazily leaves the stale fingerprint in ghost_fifo_; it ages out with the
  // FIFO and the set lookup stays correct
  return ghost_.erase(fingerprint) != 0;
}

}  // namespace cache
}  // namespace dingofs
