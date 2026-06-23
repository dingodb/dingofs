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

#ifndef DINGOFS_TEST_INTEGRATION_CACHE_LOCAL_FIXTURE_H_
#define DINGOFS_TEST_INTEGRATION_CACHE_LOCAL_FIXTURE_H_

#include <gtest/gtest.h>

#include <filesystem>
#include <string>

#include "test/integration/cache/deploy/client.h"
#include "test/integration/cache/deploy/deploy_base.h"
#include "test/integration/cache/deploy/rdma_env.h"

namespace dingofs {
namespace cache {
namespace integration {

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
// its own flags (cache_size_mb / cache_expire_s / ...) and then opens, so it
// can drive eviction / expiry / reload behaviour.
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

}  // namespace integration
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_TEST_INTEGRATION_CACHE_LOCAL_FIXTURE_H_
