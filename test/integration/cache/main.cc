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

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "test/integration/cache/deploy/rdma_env.h"

// Shared entry point for all cache integration suites (node / local /
// distributed). Linked from the integration_cache_deploy static library.
int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);

  // This process drives the cache client in-process and links the cache-node
  // gflags, some of which carry non-empty validators. Seed valid values by name
  // before ParseCommandLineFlags rejects the empty defaults.
  google::SetCommandLineOption("id", "integration-cache-driver");
  google::SetCommandLineOption("listen_ip", "127.0.0.1");
  google::SetCommandLineOption("group_name", "integration-group");

  FLAGS_minloglevel = google::GLOG_WARNING;
  FLAGS_logtostdout = true;
  FLAGS_colorlogtostdout = true;
  FLAGS_logbufsecs = 0;

  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);

  // The on-disk cache (this process AND the spawned dingo-cache) needs the
  // global RDMA slab pool. Without an IB device every fixture GTEST_SKIPs.
  if (!dingofs::cache::integration::InitSlabPoolForTests()) {
    LOG(WARNING) << "RDMA slab pool unavailable; cache integration tests SKIP.";
  } else {
    LOG(WARNING) << "RDMA slab pool ready on device="
                 << dingofs::cache::FLAGS_cache_rdma_device << ":"
                 << dingofs::cache::FLAGS_cache_rdma_port_num;
  }

  return RUN_ALL_TESTS();
}
