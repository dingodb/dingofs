// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "blockcache/infiniband/client/inflight_rpc.h"

#include <gtest/gtest.h>

#include <cstdint>

#include "blockcache/common/status.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/net/types.h"
#include "common/status.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

namespace {

using ReplyTable = InflightRpcTable<StatusOr<ReplyCode>>;

uint16_t TakeSlot(ReplyTable& table) {
  auto awaiter = table.AcquireSlot();
  EXPECT_TRUE(awaiter.await_ready());
  return awaiter.await_resume();
}

}  // namespace

TEST(InflightRpcTableTest, FailSettlesTheFutureThenFreesTheSlot) {
  ReplyTable table;
  table.Init(1);
  const uint16_t index = TakeSlot(table);
  Future<StatusOr<ReplyCode>> resp = table[index].promise.GetFuture();
  EXPECT_FALSE(table.AcquireSlot().await_ready());

  table.Fail(index, Status::Internal("send failed"));

  ASSERT_TRUE(resp.Available());
  EXPECT_FALSE(resp.Get().ok());
  EXPECT_TRUE(table.AcquireSlot().await_ready());
}

TEST(InflightRpcTableTest, FailAllSettlesEveryInflightRpcAndRecyclesSlots) {
  ReplyTable table;
  table.Init(2);
  const uint16_t a = TakeSlot(table);
  const uint16_t b = TakeSlot(table);
  Future<StatusOr<ReplyCode>> ra = table[a].promise.GetFuture();
  Future<StatusOr<ReplyCode>> rb = table[b].promise.GetFuture();

  table.FailAll(Status::Internal("connection lost"));

  EXPECT_FALSE(ra.Get().ok());
  EXPECT_FALSE(rb.Get().ok());

  // A recycled slot hands out a fresh, pending promise.
  const uint16_t c = TakeSlot(table);
  Future<StatusOr<ReplyCode>> rc = table[c].promise.GetFuture();
  EXPECT_FALSE(rc.Available());
  table[c].promise.SetValue(kReplyOk);
  ASSERT_TRUE(rc.Available());
  EXPECT_EQ(rc.Get().value(), kReplyOk);
}

// Documents why Fail() exists: recycling a slot whose rpc is still pending
// turns the Future the caller holds into a broken promise, which is fatal
// once that Future is dropped.
TEST(InflightRpcTableDeathTest, ReleasingAPendingSlotBreaksItsFuture) {
  EXPECT_DEATH(
      {
        ReplyTable table;
        table.Init(1);
        const uint16_t index = TakeSlot(table);
        Future<StatusOr<ReplyCode>> resp = table[index].promise.GetFuture();
        table.Release(index);
      },
      "broken promise");
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
