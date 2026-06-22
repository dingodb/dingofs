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

#ifndef DINGOFS_TEST_INTEGRATION_CACHE_DEPLOY_FIXTURE_H_
#define DINGOFS_TEST_INTEGRATION_CACHE_DEPLOY_FIXTURE_H_

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <string>

#include "common/options/cache.h"
#include "test/integration/cache/deploy/cache_node.h"
#include "test/integration/cache/deploy/client.h"
#include "test/integration/cache/deploy/mds_server.h"
#include "test/integration/cache/deploy/rdma_env.h"
#include "utils/uuid.h"

namespace dingofs {
namespace cache {
namespace integration {

// Shared deployment helpers for the integration fixtures. Owns a per-test
// working directory and restores all mutated gflags on teardown.
class DeployBase {
 protected:
  std::string MakeWorkDir() {
    namespace fs = std::filesystem;
    auto p = fs::temp_directory_path() /
             ("dingofs_it_" + utils::GenerateUUID());
    std::error_code ec;
    fs::create_directories(p, ec);
    workdir_ = p.string();
    return workdir_;
  }

  void CleanupWorkDir() {
    if (workdir_.empty()) return;
    std::error_code ec;
    std::filesystem::remove_all(workdir_, ec);
  }

  // Common client-side cache flags. Caller sets cache_group separately.
  void SetClientCacheFlags(const std::string& mds_addr,
                           const std::string& cache_dir) {
    FLAGS_mds_addrs = mds_addr;
    FLAGS_cache_store = "disk";
    FLAGS_cache_dir = cache_dir;
    FLAGS_cache_dir_uuid = utils::GenerateUUID();
    FLAGS_cache_size_mb = 1024;
    FLAGS_enable_stage = true;
    FLAGS_enable_cache = true;
    FLAGS_free_space_ratio = 0.0;
    FLAGS_cache_expire_s = 86400;
    FLAGS_cache_cleanup_expire_interval_ms = 100;
    FLAGS_small_block_size_kb = 0;
    // Discover cache-group members quickly so remote ops do not wait a full
    // default sync interval before the freshly-joined node shows up.
    FLAGS_periodic_sync_members_ms = 200;
  }

  // Starts a cache MDSClient, retrying until MDS discovery succeeds (the dummy
  // MDS self-registers on its first heartbeat, which lands shortly after the
  // RPC port opens).
  std::unique_ptr<MDSClientImpl> StartMdsClient() {
    for (int i = 0; i < 100; ++i) {
      auto client = std::make_unique<MDSClientImpl>();
      if (client->Start().ok()) {
        return client;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return nullptr;
  }

  gflags::FlagSaver flag_saver_;  // destroyed last -> restores flags
  std::string workdir_;
};

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
    ASSERT_TRUE(mds_.CreateLocalFs("itfs", workdir_ + "/storage", &fs_id_).ok());
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

// Fixture for the local-cache suite. Needs no cache node: the client drives a
// local on-disk TierBlockCache backed by a local-file storage dir.
class LocalCacheTest : public DeployBase, public ::testing::Test {
 protected:
  void SetUp() override {
    if (!SlabPoolReady()) {
      GTEST_SKIP() << "integration cache tests require an RDMA device";
    }
    MakeWorkDir();
    storage_dir_ = workdir_ + "/storage";
    std::error_code ec;
    std::filesystem::create_directories(storage_dir_, ec);

    // Local-only: empty cache group, no MDS needed for the backend (the client
    // holds the local-file accesser directly).
    SetClientCacheFlags(/*mds_addr=*/"", workdir_ + "/cache");
    FLAGS_cache_group = "";
    ASSERT_TRUE(client_.Open(storage_dir_).ok());
  }

  void TearDown() override {
    client_.Close();
    CleanupWorkDir();
  }

  static constexpr uint32_t kFsId = 1;
  std::string storage_dir_;
  CacheClient client_;
};

// Like LocalCacheTest but does NOT open the client in SetUp -- the test sets
// its own flags (cache_size_mb / cache_expire_s / ...) and then opens, so it can
// drive eviction / expiry / reload behaviour.
class LocalCacheRawTest : public DeployBase, public ::testing::Test {
 protected:
  void SetUp() override {
    if (!SlabPoolReady()) {
      GTEST_SKIP() << "integration cache tests require an RDMA device";
    }
    MakeWorkDir();
    storage_dir_ = workdir_ + "/storage";
    std::error_code ec;
    std::filesystem::create_directories(storage_dir_, ec);
    SetClientCacheFlags(/*mds_addr=*/"", workdir_ + "/cache");
    FLAGS_cache_group = "";
  }

  void TearDown() override {
    client_.Close();
    CleanupWorkDir();
  }

  static constexpr uint32_t kFsId = 1;
  std::string storage_dir_;
  CacheClient client_;
};

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
    ASSERT_TRUE(node_.Start(workdir_ + "/node", mds_.Addr(), kGroup, node_id_,
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

// Like DistributedCacheTest but the node runs with a tiny cache budget so writes
// far exceeding it force node-side eviction.
class DistributedSmallCacheTest : public DistributedCacheTest {
 protected:
  uint64_t NodeCacheSizeMb() const override { return 8; }
};

}  // namespace integration
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_TEST_INTEGRATION_CACHE_DEPLOY_FIXTURE_H_
