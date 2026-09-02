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
 * Created Date: 2026-09-02
 * Author: AI
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "cache/common/slab_pool.h"
#include "cache/helper/infiniband.h"
#include "cache/infiniband/common.h"
#include "cache/infiniband/sender.h"
#include "common/io_buffer.h"

namespace dingofs {
namespace cache {
namespace infiniband {

using test::MakeRegion;
using test::Rejects;

TEST(ResponseSenderTest, CheckAttachment) {
  std::vector<char> storage(8, 'x');
  auto noop = [](void*) {};

  IOBuffer registered;
  registered.AppendUserDataWithMeta(storage.data(), 8, noop, 0x1234);

  IOBuffer unregistered(storage.data(), 8);

  IOBuffer two_blocks;
  two_blocks.AppendUserDataWithMeta(storage.data(), 4, noop, 0x1234);
  two_blocks.AppendUserDataWithMeta(storage.data() + 4, 4, noop, 0x1234);

  Region dest = MakeRegion(0x1000, 8, 7);

  {  // an empty attachment is legal: error replies carry no body
    EXPECT_TRUE(ResponseSender::CheckAttachment(IOBuffer(), dest).ok());
    EXPECT_TRUE(ResponseSender::CheckAttachment(IOBuffer(), Region()).ok());
  }

  {  // source must be one registered block
    EXPECT_TRUE(Rejects(ResponseSender::CheckAttachment(two_blocks, dest),
                        "not continuous"));
    EXPECT_TRUE(Rejects(ResponseSender::CheckAttachment(unregistered, dest),
                        "not register"));
  }

  {  // source must fit the advertised region
    EXPECT_TRUE(Rejects(
        ResponseSender::CheckAttachment(registered, MakeRegion(0x1000, 4, 7)),
        "exceeds advertised rdma length"));
    EXPECT_TRUE(
        ResponseSender::CheckAttachment(registered, MakeRegion(0x1000, 16, 7))
            .ok());
  }

  {  // the incident: client advertised a region without addr or rkey
    EXPECT_TRUE(Rejects(
        ResponseSender::CheckAttachment(registered, MakeRegion(0, 8, 7)),
        "response rdma memory context is missing"));
    EXPECT_TRUE(Rejects(
        ResponseSender::CheckAttachment(registered, MakeRegion(0x1000, 8, 0)),
        "response rdma memory context is missing"));
  }

  EXPECT_TRUE(ResponseSender::CheckAttachment(registered, dest).ok());
}

TEST(ResponseSenderTest, CheckAttachmentAcceptsSlabPoolBuffer) {
  auto pool = SlabPool::Create(1);
  ASSERT_NE(pool, nullptr);
  pool->SetRdmaKeys(0x42, 0x43);

  IOBuffer buffer;
  auto lease = pool->Acquire(4096);
  ASSERT_TRUE(lease.ok());
  lease.MoveInto(&buffer, 4096);

  EXPECT_TRUE(
      ResponseSender::CheckAttachment(buffer, MakeRegion(0x1000, 4096, 7))
          .ok());
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
