/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-09-02
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/src/client/blockcache/lru_cache.h"

#include <cassert>

#include "absl/cleanup/cleanup.h"
#include "curvefs/src/base/time/time.h"
#include "curvefs/src/client/blockcache/lru_common.h"

namespace curvefs {
namespace client {
namespace blockcache {

using ::curvefs::base::cache::NewLRUCache;
using ::curvefs::base::time::TimeNow;

CacheItems LRUCache::evicted_items_ = CacheItems();

LRUCache::LRUCache() {
  // naming hash? yeah, it manage kv mapping and value's life cycle
  hash_ = NewLRUCache(1 << 30);  // large enough
  ListInit(&inactive_);
  ListInit(&active_);
}

LRUCache::~LRUCache() {
  EvictAllNodes(&inactive_);
  EvictAllNodes(&active_);
  evicted_items_.clear();
  delete hash_;
}

void LRUCache::Add(const CacheKey& key, const CacheValue& value) {
  ListNode* node = new ListNode(value);
  Insert(key.Filename(), node);
  ListAddFront(&inactive_, node);
}

bool LRUCache::Get(const CacheKey& key, CacheValue* value) {
  ListNode* node;
  bool find = Lookup(key.Filename(), &node);
  if (!find) {
    return false;
  }

  ListRemove(node);
  ListAddFront(&active_, node);
  node->value.atime = TimeNow();  // update access time
  *value = node->value;
  return true;
}

std::shared_ptr<CacheItems> LRUCache::Evict(FilterFunc filter) {
  evicted_items_.clear();
  if (EvictNode(&inactive_, filter)) {  // continue
    EvictNode(&active_, filter);
  }
  return std::make_shared<CacheItems>(std::move(evicted_items_));
}

size_t LRUCache::Size() { return hash_->TotalCharge(); }

void LRUCache::EvictAllNodes(ListNode* list) {
  ListNode* curr = list->next;
  while (curr != list) {
    ListNode* next = curr->next;
    ListRemove(curr);
    Delete(curr);
    curr = next;
  }
}

bool LRUCache::EvictNode(ListNode* list, FilterFunc filter) {
  ListNode* curr = list->next;
  while (curr != list) {
    ListNode* next = curr->next;
    auto rc = filter(curr->value);
    if (rc == FilterStatus::EVICT_IT) {
      ListRemove(curr);
      Delete(curr);
    } else if (rc == FilterStatus::SKIP) {
      // do nothing
    } else if (rc == FilterStatus::FINISH) {
      return false;
    } else {
      assert(false);  // never happen
    }

    curr = next;
  }
  return true;
}

void LRUCache::DeleteNode(const std::string_view& key, void* value) {
  ListNode* node = reinterpret_cast<ListNode*>(value);

  CacheKey k;
  assert(k.ParseFilename(key));
  evicted_items_.emplace_back(CacheItem(k, node->value));

  delete node;
}

void LRUCache::Insert(const std::string& key, ListNode* node) {
  auto* handle = hash_->Insert(key, node, 1, &LRUCache::DeleteNode);
  node->handle = handle;
}

bool LRUCache::Lookup(const std::string& key, ListNode** node) {
  auto* handle = hash_->Lookup(key);
  if (nullptr == handle) {
    return false;
  }
  *node = reinterpret_cast<ListNode*>(hash_->Value(handle));
  hash_->Release(handle);
  return true;
}

void LRUCache::Delete(ListNode* node) {
  hash_->Release(node->handle);
  hash_->Prune();  // invoke DeleteNode for all evitected cache node
}

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs
