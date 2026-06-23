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

#include <gtest/gtest.h>

#include <vector>

#include "cache/common/slab_buffer.h"
#include "cache/infiniband/common.h"
#include "cache/infiniband/controller.h"
#include "cache/infiniband/memory.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

TEST(InfinibandCommonTest, ToPbRegion) {
  Region region;
  region.addr = 0x3000;
  region.length = 256;
  region.rkey = 11;

  pb::infiniband::RDMARegion pb;
  ToPbRegion(region, &pb);
  EXPECT_EQ(pb.addr(), 0x3000u);
  EXPECT_EQ(pb.length(), 256u);
  EXPECT_EQ(pb.rkey(), 11u);
}

TEST(InfinibandCommonTest, RegionsPbRoundTrip) {
  std::vector<Region> regions(2);
  regions[0].addr = 0x1000;
  regions[0].length = 64;
  regions[0].rkey = 7;
  regions[1].addr = 0x2000;
  regions[1].length = 128;
  regions[1].rkey = 9;

  google::protobuf::RepeatedPtrField<pb::infiniband::RDMARegion> pb_regions;
  ToPbRegions(regions, &pb_regions);
  ASSERT_EQ(pb_regions.size(), 2);
  EXPECT_EQ(pb_regions.Get(0).addr(), 0x1000u);
  EXPECT_EQ(pb_regions.Get(1).rkey(), 9u);

  auto back = FromPbRegions(pb_regions);
  ASSERT_EQ(back.size(), 2u);
  EXPECT_EQ(back[0].addr, 0x1000u);
  EXPECT_EQ(back[0].length, 64u);
  EXPECT_EQ(back[1].addr, 0x2000u);
  EXPECT_EQ(back[1].rkey, 9u);
}

TEST(InfinibandCommonTest, ToPbRegionsClearsExisting) {
  google::protobuf::RepeatedPtrField<pb::infiniband::RDMARegion> pb_regions;
  pb_regions.Add();  // stale entry that must be cleared

  ToPbRegions({}, &pb_regions);
  EXPECT_EQ(pb_regions.size(), 0);
}

TEST(InfinibandCommonTest, InflightContextInitialState) {
  InflightContext ctx(3);
  EXPECT_TRUE(ctx.status.ok());
  EXPECT_EQ(ctx.contexts.size(), 3u);
}

TEST(InfinibandCommonTest, SetFailed) {
  Controller cntl;
  SetFailed(&cntl, pb::infiniband::ErrorCode::Unknown, "boom");
  EXPECT_TRUE(cntl.Failed());
  EXPECT_EQ(cntl.ErrorText(), "boom");
  EXPECT_EQ(cntl.ErrorCode(), pb::infiniband::ErrorCode::Unknown);
}

TEST(InfinibandMemoryTest, MissingRegistrationReturnsNotFoundAndZeroRkey) {
  char buffer[16] = {};

  EXPECT_TRUE(DeregisterMemoryForRDMA("missing-device", buffer).IsNotFound());
  EXPECT_EQ(GetRkey("missing-device", buffer, sizeof(buffer)), 0u);
}

TEST(InfinibandSlabPoolTest, GlobalGettersAreStable) {
  EXPECT_EQ(GetGlobalReadSlabPool(), GetGlobalReadSlabPool());
  EXPECT_EQ(GetGlobalWriteSlabPool(), GetGlobalWriteSlabPool());
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
