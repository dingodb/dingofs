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
 * Created Date: 2026-10-04
 * Author: AI
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "blockcache/core/filesystem/io_ring.h"
#include "blockcache/utils/park_queue.h"

namespace dingofs {
namespace blockcache {

// The io ring parks its ops here while it has no room for them.
class ParkedOp final : public RingOp {
 public:
  explicit ParkedOp(int id = 0) : id(id) {}

  void Issue() override {}

  int id;
};

static std::vector<ParkedOp> MakeOps(int n) {
  std::vector<ParkedOp> ops(n);
  for (int i = 0; i < n; i++) {
    ops[i].id = i;
  }
  return ops;
}

static int GetId(RingOp* op) { return static_cast<ParkedOp*>(op)->id; }

TEST(ParkQueueTest, PushBackAndPopFront) {
  std::vector<ParkedOp> ops = MakeOps(3);
  ParkQueue<RingOp> queue;

  {  // first in, first out, and a popped op is unlinked
    for (ParkedOp& op : ops) {
      queue.PushBack(&op);
    }
    for (int i = 0; i < 3; i++) {
      RingOp* op = queue.PopFront();
      ASSERT_NE(op, nullptr);
      EXPECT_EQ(GetId(op), i);
      EXPECT_EQ(op->park_next, nullptr);
    }
    EXPECT_EQ(queue.PopFront(), nullptr);
  }

  {  // an op can park again once it was popped, behind whoever came after it
    queue.PushBack(&ops[1]);
    queue.PushBack(&ops[2]);
    RingOp* first = queue.PopFront();
    queue.PushBack(first);
    EXPECT_EQ(GetId(queue.PopFront()), 2);
    EXPECT_EQ(GetId(queue.PopFront()), 1);
    EXPECT_EQ(queue.PopFront(), nullptr);
  }
}

TEST(ParkQueueTest, ConsumeAll) {
  ParkQueue<RingOp> queue;

  {  // every op, oldest first, with the queue already empty while they run;
     // returns how many there were
    std::vector<ParkedOp> ops = MakeOps(3);
    for (ParkedOp& op : ops) {
      queue.PushBack(&op);
    }
    std::vector<int> ids;
    const uint32_t n = queue.ConsumeAll([&](RingOp* op) {
      EXPECT_TRUE(queue.empty());
      EXPECT_EQ(queue.size(), 0);
      ids.push_back(GetId(op));
    });
    EXPECT_EQ(n, 3);
    EXPECT_EQ(ids, (std::vector<int>{0, 1, 2}));
  }

  {  // none parked
    EXPECT_EQ(queue.ConsumeAll([](RingOp* /*op*/) {}), 0);
  }

  {  // f may park the op again
    std::vector<ParkedOp> ops = MakeOps(3);
    for (ParkedOp& op : ops) {
      queue.PushBack(&op);
    }
    std::vector<int> ids;
    queue.ConsumeAll([&](RingOp* op) {
      ids.push_back(GetId(op));
      queue.PushBack(op);
    });
    EXPECT_EQ(ids, (std::vector<int>{0, 1, 2}));
    EXPECT_EQ(queue.size(), 3);
    EXPECT_EQ(queue.ConsumeAll([](RingOp* /*op*/) {}), 3);
  }
}

TEST(ParkQueueTest, FrontEmptyAndSize) {
  std::vector<ParkedOp> ops = MakeOps(3);
  ParkQueue<RingOp> queue;
  EXPECT_EQ(queue.front(), nullptr);
  EXPECT_TRUE(queue.empty());
  EXPECT_EQ(queue.size(), 0);

  {  // front() shows who is next without taking it
    queue.PushBack(&ops[2]);
    queue.PushBack(&ops[1]);
    EXPECT_EQ(GetId(queue.front()), 2);
    EXPECT_EQ(GetId(queue.front()), 2);
    EXPECT_FALSE(queue.empty());
    EXPECT_EQ(queue.size(), 2);
  }

  {  // and they follow every pop
    queue.PopFront();
    EXPECT_EQ(GetId(queue.front()), 1);
    EXPECT_EQ(queue.size(), 1);
    queue.PopFront();
    EXPECT_EQ(queue.front(), nullptr);
    EXPECT_TRUE(queue.empty());
    EXPECT_EQ(queue.size(), 0);
  }
}

}  // namespace blockcache
}  // namespace dingofs
