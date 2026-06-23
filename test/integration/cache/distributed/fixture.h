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

#ifndef DINGOFS_TEST_INTEGRATION_CACHE_DISTRIBUTED_FIXTURE_H_
#define DINGOFS_TEST_INTEGRATION_CACHE_DISTRIBUTED_FIXTURE_H_

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>

#include "test/integration/cache/deploy/cache_node.h"
#include "test/integration/cache/deploy/client.h"
#include "test/integration/cache/deploy/deploy_base.h"
#include "test/integration/cache/deploy/mds_server.h"
#include "test/integration/cache/deploy/rdma_env.h"
#include "utils/uuid.h"

namespace dingofs {
namespace cache {
namespace integration {

// Fixture for the distributed-cache suite. Brings up MDS + localfile fs + a
// cache node, then a client whose TierBlockCache talks to the node over TCP.
class DistributedCacheTest : public DeployBase, public ::testing::Test {
 protected:
  void SetUp() override {
    if (!SlabPoolReady()) {
      GTEST_SKIP() << "integration cache tests require an RDMA device";
    }
    MakeWorkDir();
    storage_dir_ = workdir_ + "/storage";

    ASSERT_TRUE(mds_.Start(workdir_).ok());
    ASSERT_TRUE(mds_.CreateLocalFs("itfs", storage_dir_, &fs_id_).ok());

    node_id_ = utils::GenerateUUID();
    ASSERT_TRUE(node_
                    .Start(workdir_ + "/node", mds_.Addr(), kGroup, node_id_,
                           NodeCacheSizeMb())
                    .ok());

    SetClientCacheFlags(mds_.Addr(), workdir_ + "/client-cache");
    FLAGS_cache_group = kGroup;

    mds_client_ = StartMdsClient();
    ASSERT_NE(mds_client_, nullptr);
    ASSERT_TRUE(WaitMemberOnline(mds_client_.get(), kGroup, node_id_))
        << "cache node did not register online";

    // Prefix the client's backend with the fs name so client-seeded blocks land
    // where the spawned node (which uses NewPrefixBlockAccesser) reads them.
    ASSERT_TRUE(client_.Open(storage_dir_, "itfs").ok());
  }

  void TearDown() override {
    client_.Close();
    if (mds_client_) mds_client_->Shutdown();
    node_.Stop();
    mds_.Stop();
    CleanupWorkDir();
  }

  // On-disk cache budget for the fixture's node; override to drive node-side
  // eviction.
  virtual uint64_t NodeCacheSizeMb() const { return 1024; }

  static constexpr char kGroup[] = "itgroup";
  MdsServer mds_;
  CacheNode node_;
  std::unique_ptr<MDSClientImpl> mds_client_;
  CacheClient client_;
  std::string node_id_;
  std::string storage_dir_;
  uint32_t fs_id_{0};
};

// Like DistributedCacheTest but the node runs with a tiny cache budget so
// writes far exceeding it force node-side eviction.
class DistributedSmallCacheTest : public DistributedCacheTest {
 protected:
  uint64_t NodeCacheSizeMb() const override { return 8; }
};

}  // namespace integration
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_TEST_INTEGRATION_CACHE_DISTRIBUTED_FIXTURE_H_
