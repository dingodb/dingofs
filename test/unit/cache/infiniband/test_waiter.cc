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
 * Created Date: 2026-06-21
 * Author: AI
 */

// waiter.h is not self-contained: it uses bthread::Mutex, BAIDU_SCOPED_LOCK and
// the STL containers without including them, so pull the deps in first.
#include <bthread/countdown_event.h>
#include <bthread/mutex.h>
#include <butil/scoped_lock.h>
#include <gtest/gtest.h>

#include <array>
#include <set>
#include <unordered_map>
#include <vector>

#include "cache/infiniband/waiter.h"

namespace dingofs {
namespace cache {
namespace infiniband {

TEST(WaitersTest, AddTakeRemove) {
  Waiters waiters;
  Waiter w1;
  w1.correlation_id = 100;
  Waiter w2;
  w2.correlation_id = 200;

  waiters.Add(100, &w1);
  waiters.Add(200, &w2);

  {  // Take returns and removes
    EXPECT_EQ(waiters.Take(100), &w1);
    EXPECT_EQ(waiters.Take(100), nullptr);  // already taken
  }

  {  // Remove
    EXPECT_TRUE(waiters.Remove(200));
    EXPECT_FALSE(waiters.Remove(200));  // gone
    EXPECT_EQ(waiters.Take(200), nullptr);
  }

  EXPECT_EQ(waiters.Take(999), nullptr);  // never added
}

TEST(WaitersTest, GetAll) {
  Waiters waiters;
  std::vector<Waiter> storage(64);
  for (size_t i = 0; i < storage.size(); ++i) {
    storage[i].correlation_id = i + 1;
    waiters.Add(i + 1, &storage[i]);
  }

  std::vector<Waiter*> all;
  waiters.GetAll(&all);
  EXPECT_EQ(all.size(), storage.size());

  std::set<uint64_t> ids;
  for (auto* w : all) {
    ids.insert(w->correlation_id);
  }
  EXPECT_EQ(ids.size(), storage.size());
}

TEST(WaitersTest, DifferentIdsLandConsistently) {
  // Repeated lookups for the same id must hit the same shard/entry.
  Waiters waiters;
  Waiter w;
  w.correlation_id = 0xdeadbeef;
  waiters.Add(0xdeadbeef, &w);
  EXPECT_TRUE(waiters.Remove(0xdeadbeef));
  EXPECT_FALSE(waiters.Remove(0xdeadbeef));
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
