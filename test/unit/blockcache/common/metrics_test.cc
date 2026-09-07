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

#include "blockcache/common/metrics.h"

#include <bvar/variable.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "blockcache/core/runtime/bootstrap.h"
#include "blockcache/core/runtime/smp.h"
#include "common/options/cache.h"

namespace dingofs {
namespace blockcache {
namespace {

class MetricsTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    FLAGS_shards = 2;
    StartProcessRuntime();
  }
  static void TearDownTestSuite() { StopProcessRuntime(); }

  static std::string Describe(const std::string& name) {
    return bvar::Variable::describe_exposed(name);
  }

  // One register -> expose -> read -> hide -> unregister cycle. The vars are
  // owned here but registered and unregistered on their shards, as a
  // DiskCache would.
  void RunCycle(uint64_t round) {
    const unsigned shards = ShardCount();
    std::vector<std::unique_ptr<DiskCacheVars>> disks(shards);

    RunOnAllAndWait([&](unsigned shard) -> Future<> {
      auto vars = std::make_unique<DiskCacheVars>();
      vars->index = 0;
      vars->dir = "/cache";
      vars->uuid = "uuid-0";
      vars->capacity_bytes = 1000;
      vars->used_bytes = 10 * (shard + 1);
      vars->cache_full = shard == 0 ? 1 : 0;
      vars->running = 1;
      RegisterDiskCacheVars(vars.get());
      disks[shard] = std::move(vars);

      ThisLocalCacheVars().load_bytes += 100 * (shard + 1);
      ThisRemoteCacheVars().hits += 1;
      return MakeReadyFuture<>();
    });

    ExposeMetrics();

    // The thread_local counters keep accumulating across cycles.
    EXPECT_EQ(Describe("dingofs_disk_cache_group_load_total_bytes"),
              std::to_string(300 * round));
    EXPECT_EQ(Describe("dingofs_remote_cache_hit_count"),
              std::to_string(2 * round));
    EXPECT_EQ(Describe("dingofs_remote_node_group_range_total_bytes"), "0");
    EXPECT_EQ(Describe("dingofs_disk_cache_0_capacity"), "2000");
    EXPECT_EQ(Describe("dingofs_disk_cache_0_used_bytes"), "30");
    EXPECT_EQ(Describe("dingofs_disk_cache_0_cache_full"), "1");
    EXPECT_EQ(Describe("dingofs_disk_cache_0_stage_full"), "0");
    EXPECT_EQ(Describe("dingofs_disk_cache_0_running_status"), "up");
    EXPECT_EQ(Describe("dingofs_disk_cache_0_healthy_status"), "normal");
    EXPECT_EQ(Describe("dingofs_disk_cache_0_dir"), "/cache");
    EXPECT_EQ(Describe("dingofs_disk_cache_0_uuid"), "uuid-0");
    EXPECT_TRUE(Describe("dingofs_disk_cache_1_capacity").empty());

    const std::vector<DiskStats> snapshot = SnapshotDisks();
    ASSERT_EQ(snapshot.size(), 1u);
    EXPECT_EQ(snapshot[0].index, 0u);
    EXPECT_EQ(snapshot[0].uuid, "uuid-0");
    EXPECT_EQ(snapshot[0].capacity_bytes, 2000u);
    EXPECT_EQ(snapshot[0].used_bytes, 30u);
    EXPECT_TRUE(snapshot[0].cache_full);
    EXPECT_FALSE(snapshot[0].stage_full);
    EXPECT_TRUE(snapshot[0].running);

    HideMetrics();
    EXPECT_TRUE(Describe("dingofs_disk_cache_group_load_total_bytes").empty());
    EXPECT_TRUE(Describe("dingofs_disk_cache_0_capacity").empty());

    RunOnAllAndWait([&](unsigned shard) -> Future<> {
      UnregisterDiskCacheVars(disks[shard].get());
      disks[shard].reset();
      return MakeReadyFuture<>();
    });
    EXPECT_TRUE(SnapshotDisks().empty());
  }
};

// Two cycles in one process: the second must neither collide on bvar names
// nor read the addresses collected by the first.
TEST_F(MetricsTest, ExposeHideTwice) {
  RunCycle(1);
  RunCycle(2);
}

TEST_F(MetricsTest, HideWithoutExposeIsNoop) {
  HideMetrics();
  EXPECT_TRUE(SnapshotDisks().empty());
}

}  // namespace
}  // namespace blockcache
}  // namespace dingofs
