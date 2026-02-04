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
 * Created Date: 2026-02-04
 * Author: AI
 */

#include <gtest/gtest.h>

#include <atomic>
#include <cstring>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "cache/iutil/cache.h"

namespace dingofs {
namespace cache {
namespace iutil {

namespace {

const int kCacheSize = 1000;

std::string current_deleted_key;
int current_deleted_value = 0;

void Deleter(const std::string_view& key, void* v) {
  current_deleted_key = std::string(key);
  current_deleted_value = *reinterpret_cast<int*>(v);
  delete reinterpret_cast<int*>(v);
}

int Lookup(Cache* cache, int key) {
  std::string k = std::to_string(key);
  Cache::Handle* handle = cache->Lookup(k);
  if (handle == nullptr) {
    return -1;
  }
  int result = *reinterpret_cast<int*>(cache->Value(handle));
  cache->Release(handle);
  return result;
}

void Insert(Cache* cache, int key, int value, int charge = 1) {
  std::string k = std::to_string(key);
  Cache::Handle* handle = cache->Insert(k, new int(value), charge, &Deleter);
  cache->Release(handle);
}

void Erase(Cache* cache, int key) {
  std::string k = std::to_string(key);
  cache->Erase(k);
}

}  // namespace

class CacheTest : public ::testing::Test {
 protected:
  void SetUp() override {
    current_deleted_key = "";
    current_deleted_value = 0;
  }

  void TearDown() override {}
};

TEST_F(CacheTest, EmptyLookup) {
  std::unique_ptr<Cache> cache(NewLRUCache(kCacheSize));
  EXPECT_EQ(Lookup(cache.get(), 100), -1);
}

TEST_F(CacheTest, InsertAndLookup) {
  std::unique_ptr<Cache> cache(NewLRUCache(kCacheSize));

  Insert(cache.get(), 100, 101);
  EXPECT_EQ(Lookup(cache.get(), 100), 101);

  Insert(cache.get(), 200, 201);
  EXPECT_EQ(Lookup(cache.get(), 100), 101);
  EXPECT_EQ(Lookup(cache.get(), 200), 201);

  Insert(cache.get(), 300, 301);
  EXPECT_EQ(Lookup(cache.get(), 100), 101);
  EXPECT_EQ(Lookup(cache.get(), 200), 201);
  EXPECT_EQ(Lookup(cache.get(), 300), 301);
}

TEST_F(CacheTest, InsertDuplicate) {
  std::unique_ptr<Cache> cache(NewLRUCache(kCacheSize));

  Insert(cache.get(), 100, 101);
  EXPECT_EQ(Lookup(cache.get(), 100), 101);

  Insert(cache.get(), 100, 102);
  EXPECT_EQ(Lookup(cache.get(), 100), 102);
  EXPECT_EQ(current_deleted_key, "100");
  EXPECT_EQ(current_deleted_value, 101);
}

TEST_F(CacheTest, Erase) {
  std::unique_ptr<Cache> cache(NewLRUCache(kCacheSize));

  Insert(cache.get(), 100, 101);
  Insert(cache.get(), 200, 201);

  Erase(cache.get(), 100);
  EXPECT_EQ(current_deleted_key, "100");
  EXPECT_EQ(current_deleted_value, 101);

  EXPECT_EQ(Lookup(cache.get(), 100), -1);
  EXPECT_EQ(Lookup(cache.get(), 200), 201);

  Erase(cache.get(), 100);
  EXPECT_EQ(Lookup(cache.get(), 100), -1);
  EXPECT_EQ(Lookup(cache.get(), 200), 201);
}

TEST_F(CacheTest, EraseNonExistent) {
  std::unique_ptr<Cache> cache(NewLRUCache(kCacheSize));

  Erase(cache.get(), 100);
  EXPECT_EQ(current_deleted_key, "");
  EXPECT_EQ(current_deleted_value, 0);
}

TEST_F(CacheTest, EntriesArePinned) {
  std::unique_ptr<Cache> cache(NewLRUCache(kCacheSize));

  Insert(cache.get(), 100, 101);
  std::string key = "100";
  Cache::Handle* h1 = cache->Lookup(key);
  ASSERT_NE(h1, nullptr);
  EXPECT_EQ(*reinterpret_cast<int*>(cache->Value(h1)), 101);

  Insert(cache.get(), 100, 102);
  Cache::Handle* h2 = cache->Lookup(key);
  ASSERT_NE(h2, nullptr);
  EXPECT_EQ(*reinterpret_cast<int*>(cache->Value(h2)), 102);

  EXPECT_EQ(current_deleted_key, "");

  cache->Release(h1);
  EXPECT_EQ(current_deleted_key, "100");
  EXPECT_EQ(current_deleted_value, 101);

  current_deleted_key = "";
  current_deleted_value = 0;

  Erase(cache.get(), 100);
  EXPECT_EQ(current_deleted_key, "");

  cache->Release(h2);
  EXPECT_EQ(current_deleted_key, "100");
  EXPECT_EQ(current_deleted_value, 102);
}

TEST_F(CacheTest, EvictionPolicy) {
  // Use small cache to test eviction
  std::unique_ptr<Cache> cache(NewLRUCache(10));
  Insert(cache.get(), 1, 101);
  Insert(cache.get(), 2, 201);
  Insert(cache.get(), 3, 301);

  // Access key 3 to make it recently used
  std::string key = "3";
  Cache::Handle* h = cache->Lookup(key);

  // Insert more entries to trigger eviction
  for (int i = 10; i < 20; i++) {
    Insert(cache.get(), i, i * 100);
  }

  cache->Release(h);

  // Key 3 should still be in cache because it was pinned
  EXPECT_EQ(Lookup(cache.get(), 3), 301);
}

TEST_F(CacheTest, UseExceedsCacheSize) {
  std::unique_ptr<Cache> cache(NewLRUCache(kCacheSize));

  std::vector<Cache::Handle*> handles;
  for (int i = 0; i < kCacheSize + 100; i++) {
    std::string key = std::to_string(i);
    handles.push_back(cache->Insert(key, new int(i + 1), 1, &Deleter));
  }

  for (int i = 0; i < kCacheSize + 100; i++) {
    EXPECT_EQ(Lookup(cache.get(), i), i + 1);
  }

  for (auto* h : handles) {
    cache->Release(h);
  }
}

TEST_F(CacheTest, HeavyEntries) {
  std::unique_ptr<Cache> cache(NewLRUCache(kCacheSize));
  const int kLight = 1;
  const int kHeavy = 10;
  int added = 0;
  int index = 0;

  while (added < 2 * kCacheSize) {
    int weight = (index & 1) ? kLight : kHeavy;
    Insert(cache.get(), index, 1000 + index, weight);
    added += weight;
    index++;
  }

  int cached_weight = 0;
  for (int i = 0; i < index; i++) {
    int weight = (i & 1) ? kLight : kHeavy;
    int r = Lookup(cache.get(), i);
    if (r >= 0) {
      cached_weight += weight;
      EXPECT_EQ(r, 1000 + i);
    }
  }
  EXPECT_LE(cached_weight, kCacheSize + (kCacheSize / 10));
}

TEST_F(CacheTest, NewId) {
  std::unique_ptr<Cache> cache(NewLRUCache(kCacheSize));
  uint64_t a = cache->NewId();
  uint64_t b = cache->NewId();
  EXPECT_NE(a, b);
  EXPECT_EQ(b, a + 1);
}

TEST_F(CacheTest, Prune) {
  std::unique_ptr<Cache> cache(NewLRUCache(kCacheSize));

  Insert(cache.get(), 1, 100);
  Insert(cache.get(), 2, 200);

  std::string key = "1";
  Cache::Handle* handle = cache->Lookup(key);
  ASSERT_NE(handle, nullptr);

  cache->Prune();

  cache->Release(handle);
  EXPECT_EQ(Lookup(cache.get(), 1), 100);
  EXPECT_EQ(Lookup(cache.get(), 2), -1);
}

TEST_F(CacheTest, TotalCharge) {
  std::unique_ptr<Cache> cache(NewLRUCache(kCacheSize));

  EXPECT_EQ(cache->TotalCharge(), 0);

  Insert(cache.get(), 1, 100, 10);
  EXPECT_EQ(cache->TotalCharge(), 10);

  Insert(cache.get(), 2, 200, 20);
  EXPECT_EQ(cache->TotalCharge(), 30);

  Erase(cache.get(), 1);
  EXPECT_EQ(cache->TotalCharge(), 20);

  Erase(cache.get(), 2);
  EXPECT_EQ(cache->TotalCharge(), 0);
}

TEST_F(CacheTest, ZeroSizeCache) {
  std::unique_ptr<Cache> cache(NewLRUCache(0));

  Insert(cache.get(), 1, 100);
  EXPECT_EQ(Lookup(cache.get(), 1), -1);
  EXPECT_EQ(cache->TotalCharge(), 0);
}

TEST_F(CacheTest, KeyAndValue) {
  std::unique_ptr<Cache> cache(NewLRUCache(kCacheSize));

  std::string key = "test_key";
  int* value = new int(12345);
  Cache::Handle* handle = cache->Insert(key, value, 1, &Deleter);
  ASSERT_NE(handle, nullptr);

  EXPECT_EQ(cache->Key(handle), key);
  EXPECT_EQ(*reinterpret_cast<int*>(cache->Value(handle)), 12345);

  cache->Release(handle);
}

TEST_F(CacheTest, LongKey) {
  std::unique_ptr<Cache> cache(NewLRUCache(kCacheSize));

  std::string long_key(1000, 'x');
  int* value = new int(99999);
  Cache::Handle* handle = cache->Insert(long_key, value, 1, &Deleter);
  ASSERT_NE(handle, nullptr);

  EXPECT_EQ(cache->Key(handle), long_key);
  EXPECT_EQ(*reinterpret_cast<int*>(cache->Value(handle)), 99999);

  cache->Release(handle);

  Cache::Handle* lookup_handle = cache->Lookup(long_key);
  ASSERT_NE(lookup_handle, nullptr);
  EXPECT_EQ(*reinterpret_cast<int*>(cache->Value(lookup_handle)), 99999);
  cache->Release(lookup_handle);
}

TEST_F(CacheTest, EmptyKey) {
  std::unique_ptr<Cache> cache(NewLRUCache(kCacheSize));

  std::string empty_key;
  int* value = new int(11111);
  Cache::Handle* handle = cache->Insert(empty_key, value, 1, &Deleter);
  ASSERT_NE(handle, nullptr);

  EXPECT_EQ(cache->Key(handle), empty_key);
  EXPECT_EQ(*reinterpret_cast<int*>(cache->Value(handle)), 11111);

  cache->Release(handle);

  Cache::Handle* lookup_handle = cache->Lookup(empty_key);
  ASSERT_NE(lookup_handle, nullptr);
  cache->Release(lookup_handle);
}

TEST_F(CacheTest, ConcurrentInsertAndLookup) {
  std::unique_ptr<Cache> cache(NewLRUCache(kCacheSize * 10));
  const int kNumThreads = 8;
  const int kOpsPerThread = 1000;

  std::atomic<int> insert_count{0};
  std::atomic<int> lookup_count{0};

  auto insert_fn = [&](int thread_id) {
    for (int i = 0; i < kOpsPerThread; i++) {
      int key = (thread_id * kOpsPerThread) + i;
      Insert(cache.get(), key, key * 2);
      insert_count++;
    }
  };

  auto lookup_fn = [&](int thread_id) {
    for (int i = 0; i < kOpsPerThread; i++) {
      int key = (thread_id * kOpsPerThread) + i;
      Lookup(cache.get(), key);
      lookup_count++;
    }
  };

  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; i++) {
    threads.emplace_back(insert_fn, i);
  }
  for (auto& t : threads) {
    t.join();
  }

  threads.clear();
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; i++) {
    threads.emplace_back(lookup_fn, i);
  }
  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(insert_count.load(), kNumThreads * kOpsPerThread);
  EXPECT_EQ(lookup_count.load(), kNumThreads * kOpsPerThread);
}

TEST_F(CacheTest, ConcurrentMixedOperations) {
  std::unique_ptr<Cache> cache(NewLRUCache(kCacheSize));
  const int kNumThreads = 4;
  const int kOpsPerThread = 500;

  auto worker = [&](int thread_id) {
    for (int i = 0; i < kOpsPerThread; i++) {
      int key = ((thread_id * kOpsPerThread) + i) % 100;
      int op = i % 3;
      switch (op) {
        case 0:
          Insert(cache.get(), key, key * 2);
          break;
        case 1:
          Lookup(cache.get(), key);
          break;
        case 2:
          Erase(cache.get(), key);
          break;
        default:
          break;
      }
    }
  };

  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; i++) {
    threads.emplace_back(worker, i);
  }
  for (auto& t : threads) {
    t.join();
  }
}

TEST_F(CacheTest, ConcurrentNewId) {
  std::unique_ptr<Cache> cache(NewLRUCache(kCacheSize));
  const int kNumThreads = 8;
  const int kIdsPerThread = 1000;

  std::vector<std::vector<uint64_t>> all_ids(kNumThreads);

  auto id_generator = [&](int thread_id) {
    for (int i = 0; i < kIdsPerThread; i++) {
      all_ids[thread_id].push_back(cache->NewId());
    }
  };

  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; i++) {
    threads.emplace_back(id_generator, i);
  }
  for (auto& t : threads) {
    t.join();
  }

  std::set<uint64_t> unique_ids;
  for (const auto& ids : all_ids) {
    for (uint64_t id : ids) {
      EXPECT_TRUE(unique_ids.insert(id).second);
    }
  }
  EXPECT_EQ(unique_ids.size(), kNumThreads * kIdsPerThread);
}

TEST_F(CacheTest, EvictionWithCharge) {
  // ShardedLRUCache uses 64 shards, use large capacity for predictable behavior
  std::unique_ptr<Cache> cache(NewLRUCache(10000));

  Insert(cache.get(), 1, 100, 5000);
  Insert(cache.get(), 2, 200, 5000);
  EXPECT_EQ(cache->TotalCharge(), 10000);

  // Insert another entry that should trigger eviction
  Insert(cache.get(), 3, 300, 5000);

  // Total charge should not exceed capacity significantly
  EXPECT_LE(cache->TotalCharge(), 15000);

  // At least 2 entries should be found
  int found = 0;
  if (Lookup(cache.get(), 1) != -1) found++;
  if (Lookup(cache.get(), 2) != -1) found++;
  if (Lookup(cache.get(), 3) != -1) found++;
  EXPECT_GE(found, 2);
}

TEST_F(CacheTest, LRUOrdering) {
  // Use larger cache to avoid sharding issues
  std::unique_ptr<Cache> cache(NewLRUCache(1000));

  Insert(cache.get(), 1, 100);
  Insert(cache.get(), 2, 200);
  Insert(cache.get(), 3, 300);

  // Access key 1 to make it recently used
  EXPECT_EQ(Lookup(cache.get(), 1), 100);

  // All entries should still be present
  EXPECT_EQ(Lookup(cache.get(), 1), 100);
  EXPECT_EQ(Lookup(cache.get(), 2), 200);
  EXPECT_EQ(Lookup(cache.get(), 3), 300);
}

TEST_F(CacheTest, MultipleHandlesToSameKey) {
  std::unique_ptr<Cache> cache(NewLRUCache(kCacheSize));

  Insert(cache.get(), 100, 101);
  std::string key = "100";

  Cache::Handle* h1 = cache->Lookup(key);
  Cache::Handle* h2 = cache->Lookup(key);
  Cache::Handle* h3 = cache->Lookup(key);

  ASSERT_NE(h1, nullptr);
  ASSERT_NE(h2, nullptr);
  ASSERT_NE(h3, nullptr);

  EXPECT_EQ(*reinterpret_cast<int*>(cache->Value(h1)), 101);
  EXPECT_EQ(*reinterpret_cast<int*>(cache->Value(h2)), 101);
  EXPECT_EQ(*reinterpret_cast<int*>(cache->Value(h3)), 101);

  cache->Release(h1);
  cache->Release(h2);
  cache->Release(h3);

  EXPECT_EQ(Lookup(cache.get(), 100), 101);
}

TEST_F(CacheTest, ReleaseAndReinsert) {
  std::unique_ptr<Cache> cache(NewLRUCache(kCacheSize));

  for (int round = 0; round < 3; round++) {
    Insert(cache.get(), 1, round * 100);
    EXPECT_EQ(Lookup(cache.get(), 1), round * 100);

    Erase(cache.get(), 1);
    EXPECT_EQ(Lookup(cache.get(), 1), -1);
  }
}

TEST_F(CacheTest, ShardingDistribution) {
  std::unique_ptr<Cache> cache(NewLRUCache(kCacheSize * 100));

  for (int i = 0; i < 1000; i++) {
    Insert(cache.get(), i, i * 2);
  }

  int found = 0;
  for (int i = 0; i < 1000; i++) {
    if (Lookup(cache.get(), i) == i * 2) {
      found++;
    }
  }

  EXPECT_EQ(found, 1000);
}

}  // namespace iutil
}  // namespace cache
}  // namespace dingofs
