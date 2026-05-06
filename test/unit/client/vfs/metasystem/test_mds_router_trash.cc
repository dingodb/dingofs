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

#include <gtest/gtest.h>

#include "client/vfs/metasystem/mds/mds_discovery.h"
#include "client/vfs/metasystem/mds/mds_router.h"
#include "client/vfs/metasystem/mds/parent_memo.h"
#include "client/vfs/metasystem/mds/rpc.h"
#include "mds/common/trash.h"
#include "mds/mds/mds_meta.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {
namespace test {

// All inodes with value >= kTrashInodeId (i.e. .trash root and every
// hour-bucket) must short-circuit out of parent-hash routing so the load is
// spread across MDSes via random fallback. Cross-MDS trash-moves don't notify
// the bucket-owner, so fixed routing gives no caching benefit and concentrates
// load on one node.
class ParentHashMDSRouterTrashTest : public ::testing::Test {
 protected:
  ParentHashMDSRouterTrashTest()
      : rpc_("127.0.0.1:0"),
        mds_discovery_(rpc_),
        router_(mds_discovery_, parent_memo_) {}

  RPC rpc_;
  MDSDiscovery mds_discovery_;
  ParentMemo parent_memo_;
  ParentHashMDSRouter router_;
};

TEST_F(ParentHashMDSRouterTrashTest, GetMDSByParentTrashRoot) {
  mds::MDSMeta mds_meta;
  EXPECT_FALSE(router_.GetMDSByParent(mds::kTrashInodeId, mds_meta));
}

TEST_F(ParentHashMDSRouterTrashTest, GetMDSByParentHourBucketStart) {
  mds::MDSMeta mds_meta;
  EXPECT_FALSE(router_.GetMDSByParent(mds::kTrashSubInodeStart, mds_meta));
}

TEST_F(ParentHashMDSRouterTrashTest, GetMDSByParentHighHourBucket) {
  mds::MDSMeta mds_meta;
  EXPECT_FALSE(
      router_.GetMDSByParent(mds::kTrashSubInodeStart + 0x1000, mds_meta));
}

TEST_F(ParentHashMDSRouterTrashTest, GetMDSWithHourBucketParent) {
  // Child inode whose parent resolves to an hour-bucket must also short-circuit
  // via the same IsTrashInode branch in GetMDS.
  Ino bucket_ino = mds::kTrashSubInodeStart + 7;
  Ino child_ino = 1234567;
  parent_memo_.Upsert(child_ino, bucket_ino);

  mds::MDSMeta mds_meta;
  EXPECT_FALSE(router_.GetMDS(child_ino, mds_meta));
}

}  // namespace test
}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs
