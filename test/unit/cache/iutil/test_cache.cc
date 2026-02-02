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
 * Created Date: 2026-02-02
 * Author: AI
 */

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "cache/iutil/cache.h"

namespace dingofs {
namespace cache {
namespace iutil {

class CacheTest : public ::testing::Test {
 protected:
  static void Deleter(const std::string_view& key, void* value) {
    delete static_cast<int*>(value);
  }

  static int* NewValue(int v) { return new int(v); }

  static int GetValue(Cache::Handle* handle, Cache* cache) {
    return *static_cast<int*>(cache->Value(handle));
  }
};

TEST_F(CacheTest, NewLRUCache) {
  std::unique_ptr<Cache> cache(NewLRUCache(100));
  EXPECT_NE(cache, nullptr);
  EXPECT_EQ(cache->TotalCharge(), 0);
}

TEST_F(CacheTest, InsertAndLookup) {
  std::unique_ptr<Cache> cache(NewLRUCache(100));

  std::string key = "test_key";
  auto* handle = cache->Insert(key, NewValue(42), 1, Deleter);
  EXPECT_NE(handle, nullptr);

  auto* lookup_handle = cache->Lookup(key);
  EXPECT_NE(lookup_handle, nullptr);
  EXPECT_EQ(GetValue(lookup_handle, cache.get()), 42);

  cache->Release(handle);
  cache->Release(lookup_handle);
}

TEST_F(CacheTest, LookupNotFound) {
  std::unique_ptr<Cache> cache(NewLRUCache(100));

  auto* handle = cache->Lookup("not_exist");
  EXPECT_EQ(handle, nullptr);
}

TEST_F(CacheTest, Erase) {
  std::unique_ptr<Cache> cache(NewLRUCache(100));

  std::string key = "test_key";
  auto* handle = cache->Insert(key, NewValue(42), 1, Deleter);
  cache->Release(handle);

  cache->Erase(key);

  auto* lookup_handle = cache->Lookup(key);
  EXPECT_EQ(lookup_handle, nullptr);
}

TEST_F(CacheTest, KeyAndValue) {
  std::unique_ptr<Cache> cache(NewLRUCache(100));

  std::string key = "test_key";
  auto* handle = cache->Insert(key, NewValue(123), 1, Deleter);

  EXPECT_EQ(cache->Key(handle), key);
  EXPECT_EQ(GetValue(handle, cache.get()), 123);

  cache->Release(handle);
}

TEST_F(CacheTest, TotalCharge) {
  std::unique_ptr<Cache> cache(NewLRUCache(100));

  auto* h1 = cache->Insert("key1", NewValue(1), 10, Deleter);
  EXPECT_EQ(cache->TotalCharge(), 10);

  auto* h2 = cache->Insert("key2", NewValue(2), 20, Deleter);
  EXPECT_EQ(cache->TotalCharge(), 30);

  cache->Release(h1);
  cache->Release(h2);
}

TEST_F(CacheTest, NewId) {
  std::unique_ptr<Cache> cache(NewLRUCache(100));

  uint64_t id1 = cache->NewId();
  uint64_t id2 = cache->NewId();
  uint64_t id3 = cache->NewId();

  EXPECT_NE(id1, id2);
  EXPECT_NE(id2, id3);
  EXPECT_NE(id1, id3);
}

TEST_F(CacheTest, Prune) {
  std::unique_ptr<Cache> cache(NewLRUCache(100));

  auto* h1 = cache->Insert("key1", NewValue(1), 10, Deleter);
  auto* h2 = cache->Insert("key2", NewValue(2), 10, Deleter);

  cache->Release(h1);

  cache->Prune();

  auto* lookup1 = cache->Lookup("key1");
  EXPECT_EQ(lookup1, nullptr);

  auto* lookup2 = cache->Lookup("key2");
  EXPECT_NE(lookup2, nullptr);

  cache->Release(h2);
  cache->Release(lookup2);
}

TEST_F(CacheTest, ReplaceExistingKey) {
  std::unique_ptr<Cache> cache(NewLRUCache(100));

  std::string key = "test_key";
  auto* h1 = cache->Insert(key, NewValue(1), 10, Deleter);
  cache->Release(h1);

  auto* h2 = cache->Insert(key, NewValue(2), 10, Deleter);

  auto* lookup = cache->Lookup(key);
  EXPECT_NE(lookup, nullptr);
  EXPECT_EQ(GetValue(lookup, cache.get()), 2);

  cache->Release(h2);
  cache->Release(lookup);
}

}  // namespace iutil
}  // namespace cache
}  // namespace dingofs
