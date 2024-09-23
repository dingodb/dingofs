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

#ifndef CURVEFS_SRC_CLIENT_BLOCKCACHE_LRU_CACHE_H_
#define CURVEFS_SRC_CLIENT_BLOCKCACHE_LRU_CACHE_H_

#include <functional>
#include <memory>
#include <string_view>

#include "curvefs/src/client/blockcache/lru_common.h"

namespace curvefs {
namespace client {
namespace blockcache {

enum class FilterStatus {
  EVICT_IT,
  SKIP,
  FINISH,
};

// protect by DiskCacheManager
class LRUCache {
  using FilterFunc = std::function<FilterStatus(const CacheValue& value)>;

 public:
  LRUCache();

  virtual ~LRUCache();

  virtual void Add(const CacheKey& key, const CacheValue& value);

  virtual bool Get(const CacheKey& key, CacheValue* value);

  virtual CacheItems Evict(FilterFunc filter);

  virtual size_t Size();

  virtual void Clear();

 private:
  void EvictAllNodes(ListNode* list);

  bool EvictNode(ListNode* list, FilterFunc filter);

  static void DeleteNode(const std::string_view& key, void* value);

  void Insert(const std::string& key, ListNode* node);

  bool Lookup(const std::string& key, ListNode** node);

  void Delete(ListNode* node);

 private:
  Cache* hash_;  // mapping: CacheKey -> ListNode*
  ListNode active_;
  ListNode inactive_;
  static CacheItems evicted_items_;
};

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BLOCKCACHE_LRU_CACHE_H_
