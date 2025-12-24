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

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_LRU_CACHE_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_LRU_CACHE_H_

#include <functional>

#include "cache/blockcache/cache_store.h"
#include "cache/iutil/cache.h"
#include "cache/iutil/time_util.h"

namespace dingofs {
namespace cache {

using CacheKey = BlockKey;

struct CacheValue {
  CacheValue() = default;
  CacheValue(size_t size, iutil::TimeSpec atime) : size(size), atime(atime) {}

  size_t size;
  iutil::TimeSpec atime;  // access time
};

struct CacheItem {
  CacheItem(CacheKey key, CacheValue value) : key(key), value(value) {}

  CacheKey key;
  CacheValue value;
};

using CacheItems = std::vector<CacheItem>;

struct ListNode {
  ListNode() = default;

  ListNode(const CacheValue& value)
      : value(value), handle(nullptr), prev(nullptr), next(nullptr) {}

  CacheValue value;
  iutil::Cache::Handle* handle;
  struct ListNode* prev;
  struct ListNode* next;
};

inline void ListInit(ListNode* list) {
  list->next = list;
  list->prev = list;
}

inline void ListAddFront(ListNode* list, ListNode* node) {
  node->next = list;
  node->prev = list->prev;
  node->prev->next = node;
  node->next->prev = node;
}

inline void ListRemove(ListNode* node) {
  node->next->prev = node->prev;
  node->prev->next = node->next;
}

enum class FilterStatus : uint8_t {
  kEvictIt,
  kSkip,
  kFinish,
};

// How it implements:
//  hash table: using base::Cache
//  lru policy: manage inactive and active list
class LRUCache {
 public:
  using FilterFunc = std::function<FilterStatus(const CacheValue& value)>;

  LRUCache();

  virtual ~LRUCache();

  virtual void Add(const CacheKey& key, const CacheValue& value);
  virtual bool Get(const CacheKey& key, CacheValue* value);
  virtual bool Delete(const CacheKey& key, CacheValue* deleted);
  virtual bool Exist(const CacheKey& key);
  virtual CacheItems Evict(FilterFunc filter);

  virtual size_t Size();
  virtual void Clear();

 private:
  void HashInsert(const std::string& key, ListNode* node);
  bool HashLookup(const std::string& key, ListNode** node);
  void HashDelete(ListNode* node);

  CacheItem KV(ListNode* node);
  // return true if should continue to evict
  bool EvictNode(ListNode* list, FilterFunc filter, CacheItems* evicted);
  void EvictAllNodes(ListNode* list);

  iutil::Cache* hash_;  // mapping: CacheKey -> ListNode*
  ListNode active_;
  ListNode inactive_;
};

using LRUCacheUPtr = std::unique_ptr<LRUCache>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_LRU_CACHE_H_
