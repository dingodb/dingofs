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

#ifndef DINGOFS_TEST_INTEGRATION_CACHE_NODE_FIXTURE_H_
#define DINGOFS_TEST_INTEGRATION_CACHE_NODE_FIXTURE_H_

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>

#include "test/integration/cache/deploy/cache_node.h"
#include "test/integration/cache/deploy/client.h"
#include "test/integration/cache/deploy/deploy_base.h"
#include "test/integration/cache/deploy/mds_server.h"
#include "test/integration/cache/deploy/rdma_env.h"

namespace dingofs {
namespace cache {
namespace integration {

// Fixture for the cache-node lifecycle suite. Brings up an MDS + a localfile
// filesystem; tests start/stop the cache node themselves to observe
// registration / deregistration.
class NodeTest : public DeployBase, public ::testing::Test {
 protected:
  void SetUp() override {
    if (!SlabPoolReady()) {
      GTEST_SKIP() << "integration cache tests require an RDMA device";
    }
    MakeWorkDir();
    ASSERT_TRUE(mds_.Start(workdir_).ok());
    FLAGS_mds_addrs = mds_.Addr();
    ASSERT_TRUE(
        mds_.CreateLocalFs("itfs", workdir_ + "/storage", &fs_id_).ok());
    mds_client_ = StartMdsClient();
    ASSERT_NE(mds_client_, nullptr);
  }

  void TearDown() override {
    if (mds_client_) mds_client_->Shutdown();
    mds_.Stop();
    CleanupWorkDir();
  }

  static constexpr char kGroup[] = "itgroup";
  MdsServer mds_;
  std::unique_ptr<MDSClientImpl> mds_client_;
  uint32_t fs_id_{0};
};

}  // namespace integration
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_TEST_INTEGRATION_CACHE_NODE_FIXTURE_H_
