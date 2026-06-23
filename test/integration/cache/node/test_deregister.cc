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
 * Created Date: 2026-06-22
 * Author: AI
 */

#include "test/integration/cache/node/fixture.h"

namespace dingofs {
namespace cache {
namespace integration {

// After a node stops, it stops being Online in the group: either it leaves the
// group on shutdown, or its heartbeat lapses and the MDS demotes it from
// Online.
TEST_F(NodeTest, NoLongerOnlineAfterStop) {
  const auto id = utils::GenerateUUID();
  CacheNode node;
  ASSERT_TRUE(node.Start(workdir_ + "/node", mds_.Addr(), kGroup, id).ok());
  ASSERT_TRUE(WaitMemberOnline(mds_client_.get(), kGroup, id));

  node.Stop();
  ASSERT_FALSE(node.Running());

  EXPECT_TRUE(WaitMemberNotOnline(mds_client_.get(), kGroup, id))
      << "node still reported Online after being stopped";
}

}  // namespace integration
}  // namespace cache
}  // namespace dingofs
