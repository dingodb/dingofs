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

#include "cache/blockcache/lru_cache.h"

#include <glog/logging.h>

namespace dingofs {
namespace cache {

static void FreeNode(const std::string_view&, void* value) {
  ListNode* node = reinterpret_cast<ListNode*>(value);
  delete node;
}

LRUCache::LRUCache() {
  // naming hash? yeah, it manage kv mapping and value's life cycle
  hash_ = iutil::NewLRUCache(1 << 30);  // large enough
  ListInit(&inactive_);
  ListInit(&active_);
}

LRUCache::~LRUCache() {
  Clear();
  delete hash_;
}

void LRUCache::Add(const CacheKey& key, const CacheValue& value) {
  ListNode* node = new ListNode(value);
  HashInsert(key.Filename(), node);
  ListAddFront(&inactive_, node);
}

bool LRUCache::Get(const CacheKey& key, CacheValue* value) {
  ListNode* node;
  bool find = HashLookup(key.Filename(), &node);
  if (!find) {
    return false;
  }

  ListRemove(node);
  ListAddFront(&active_, node);
  node->value.atime = iutil::TimeNow();  // update access time
  *value = node->value;
  return true;
}

bool LRUCache::Delete(const CacheKey& key, CacheValue* deleted) {
  ListNode* node;
  bool find = HashLookup(key.Filename(), &node);
  if (!find) {
    return false;
  }

  *deleted = node->value;
  ListRemove(node);
  HashDelete(node);
  return true;
}

bool LRUCache::Exist(const CacheKey& key) {
  ListNode* node;
  return HashLookup(key.Filename(), &node);
}

CacheItems LRUCache::Evict(FilterFunc filter) {
  CacheItems evicted;
  if (EvictNode(&inactive_, filter, &evicted)) {  // continue
    EvictNode(&active_, filter, &evicted);
  }
  return evicted;
}

size_t LRUCache::Size() { return hash_->TotalCharge(); }

void LRUCache::Clear() {
  EvictAllNodes(&inactive_);
  EvictAllNodes(&active_);
}

void LRUCache::HashInsert(const std::string& key, ListNode* node) {
  auto* handle = hash_->Insert(key, node, 1, &FreeNode);
  node->handle = handle;
}

bool LRUCache::HashLookup(const std::string& key, ListNode** node) {
  auto* handle = hash_->Lookup(key);
  if (nullptr == handle) {
    return false;
  }
  *node = reinterpret_cast<ListNode*>(hash_->Value(handle));
  hash_->Release(handle);
  return true;
}

void LRUCache::HashDelete(ListNode* node) {
  hash_->Release(node->handle);
  hash_->Prune();  // invoke FreeNode for all evitected cache node
}

CacheItem LRUCache::KV(ListNode* node) {
  CacheKey key;
  // we use CacheKey.Filename() as hash key
  auto filename = hash_->Key(node->handle);
  CHECK(key.ParseFromFilename(filename)) << "filename = " << filename;
  return CacheItem(key, node->value);
}

bool LRUCache::EvictNode(ListNode* list, FilterFunc filter,
                         CacheItems* evicted) {
  ListNode* curr = list->next;
  while (curr != list) {
    ListNode* next = curr->next;
    auto rc = filter(curr->value);
    if (rc == FilterStatus::kEvictIt) {
      evicted->emplace_back(KV(curr));
      ListRemove(curr);
      HashDelete(curr);
    } else if (rc == FilterStatus::kSkip) {
      // do nothing
    } else if (rc == FilterStatus::kFinish) {
      return false;
    } else {
      CHECK(false);  // never happen
    }

    curr = next;
  }
  return true;
}

void LRUCache::EvictAllNodes(ListNode* list) {
  ListNode* curr = list->next;
  while (curr != list) {
    ListNode* next = curr->next;
    ListRemove(curr);
    HashDelete(curr);
    curr = next;
  }
}

}  // namespace cache
}  // namespace dingofs
