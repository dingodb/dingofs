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

#include <cstring>

#include "cache/infiniband/infiniband.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

TEST(ConnManagmentMetaTest, SerializeParseRoundTrip) {
  ConnManagmentMeta meta;
  meta.qpn = 0x123456;
  meta.lid = 0xABCD;
  for (int i = 0; i < 16; ++i) {
    meta.gid.raw[i] = static_cast<uint8_t>(i + 1);
  }
  meta.port_num = 1;
  meta.link_type = LinkLayer::kIB;
  meta.mtu = IBV_MTU_1024;

  pb::infiniband::ConnManagementMeta pb;
  SerializeToPb(meta, &pb);
  EXPECT_EQ(pb.qpn(), 0x123456u);
  EXPECT_EQ(pb.lid(), 0xABCDu);
  EXPECT_EQ(pb.gid().size(), sizeof(meta.gid));
  EXPECT_EQ(pb.port_num(), 1u);

  ConnManagmentMeta back;
  ASSERT_TRUE(ParseFromPb(pb, &back).ok());
  EXPECT_EQ(back.qpn, meta.qpn);
  EXPECT_EQ(back.lid, meta.lid);
  EXPECT_EQ(std::memcmp(&back.gid, &meta.gid, sizeof(ibv_gid)), 0);
  EXPECT_EQ(back.port_num, meta.port_num);
  EXPECT_EQ(back.link_type, meta.link_type);
  EXPECT_EQ(back.mtu, meta.mtu);
}

TEST(ConnManagmentMetaTest, ParseRejectsBadGidSize) {
  pb::infiniband::ConnManagementMeta pb;
  pb.set_qpn(1);
  pb.set_gid("too-short");  // not sizeof(ibv_gid)

  ConnManagmentMeta out;
  EXPECT_TRUE(ParseFromPb(pb, &out).IsInternal());
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
