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

#ifndef DINGOFS_TEST_INTEGRATION_CACHE_DEPLOY_DEPLOY_BASE_H_
#define DINGOFS_TEST_INTEGRATION_CACHE_DEPLOY_DEPLOY_BASE_H_

#include <gflags/gflags.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>

#include "cache/common/mds_client.h"
#include "common/options/cache.h"
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
    auto p =
        fs::temp_directory_path() / ("dingofs_it_" + utils::GenerateUUID());
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

}  // namespace integration
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_TEST_INTEGRATION_CACHE_DEPLOY_DEPLOY_BASE_H_
