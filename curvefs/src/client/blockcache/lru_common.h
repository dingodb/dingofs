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
 * Created Date: 2024-09-07
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_BLOCKCACHE_LRU_COMMON_H
#define CURVEFS_SRC_CLIENT_BLOCKCACHE_LRU_COMMON_H

#include <vector>

#include "curvefs/src/base/cache/cache.h"
#include "curvefs/src/base/time/time.h"
#include "curvefs/src/client/blockcache/cache_store.h"

namespace curvefs {
namespace client {
namespace blockcache {

using ::curvefs::base::cache::Cache;
using ::curvefs::base::time::TimeSpec;

using CacheKey = BlockKey;

struct CacheValue {
  CacheValue() = default;

  CacheValue(size_t size, TimeSpec atime) : size(size), atime(atime) {}

  size_t size;
  TimeSpec atime;  // access time
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
  Cache::Handle* handle;
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

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BLOCKCACHE_LRU_COMMON_H
