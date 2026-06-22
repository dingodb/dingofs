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
#include <json/value.h>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>

#include "cache/remote/remote_block_cache.h"
#include "common/block/block_handle.h"
#include "common/block/block_key.h"
#include "common/options/cache.h"

namespace dingofs {
namespace cache {

namespace {

BlockHandle Handle() { return BlockHandle(1, BlockKey(100, 0, 4096)); }

IOBuffer Buffer(const std::string& data) {
  return IOBuffer(data.data(), data.size());
}

template <typename Pred>
bool WaitUntil(Pred pred, int timeout_ms = 3000) {
  for (int waited = 0; waited < timeout_ms; waited += 20) {
    if (pred()) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  return pred();
}

}  // namespace

class RemoteBlockCacheTest : public ::testing::Test {
 protected:
  void SetUp() override { saved_cache_group_ = FLAGS_cache_group; }

  void TearDown() override { FLAGS_cache_group = saved_cache_group_; }

  void MarkRunning(RemoteBlockCache& cache) {
    cache.running_.store(true, std::memory_order_relaxed);
  }

  void StartJoinerAndMarkRunning(RemoteBlockCache& cache) {
    cache.joiner_->Start();
    MarkRunning(cache);
  }

  void MarkShutdown(RemoteBlockCache& cache) {
    cache.running_.store(false, std::memory_order_relaxed);
  }

 private:
  std::string saved_cache_group_;
};

TEST_F(RemoteBlockCacheTest, EnableFlagsFollowCacheGroup) {
  RemoteBlockCache cache(nullptr);

  FLAGS_cache_group = "";
  EXPECT_FALSE(cache.IsEnabled());
  EXPECT_FALSE(cache.EnableStage());
  EXPECT_FALSE(cache.EnableCache());
  EXPECT_FALSE(cache.IsCached(Handle()));

  FLAGS_cache_group = "cache-group-a";
  EXPECT_TRUE(cache.IsEnabled());
  EXPECT_TRUE(cache.EnableStage());
  EXPECT_TRUE(cache.EnableCache());
  EXPECT_TRUE(cache.IsCached(Handle()));
}

TEST_F(RemoteBlockCacheTest, ShutdownBeforeStartIsOk) {
  RemoteBlockCache cache(nullptr);
  EXPECT_TRUE(cache.Shutdown().ok());
}

TEST_F(RemoteBlockCacheTest, DumpDelegatesToCluster) {
  RemoteBlockCache cache(nullptr);
  Json::Value value;

  EXPECT_TRUE(cache.Dump(value));
  ASSERT_TRUE(value.isMember("members"));
  EXPECT_TRUE(value["members"].isArray());
}

TEST_F(RemoteBlockCacheTest, OperationsReturnNotFoundWithoutNode) {
  RemoteBlockCache cache(nullptr);
  MarkRunning(cache);

  EXPECT_TRUE(cache.Put(Handle(), Buffer("remote-block"), {}).IsNotFound());
  EXPECT_TRUE(cache.Cache(Handle(), Buffer("remote-block"), {}).IsNotFound());
  EXPECT_TRUE(cache.Prefetch(Handle(), 4096, {}).IsNotFound());

  std::string out(8, '\0');
  IOBuffer buffer(out.data(), out.size());
  RangeOption option;
  option.block_whole_length = 4096;

  EXPECT_TRUE(
      cache.Range(Handle(), 0, out.size(), &buffer, option).IsNotFound());

  MarkShutdown(cache);
}

TEST_F(RemoteBlockCacheTest, AsyncOperationsReturnNotFoundWithoutNode) {
  RemoteBlockCache cache(nullptr);
  StartJoinerAndMarkRunning(cache);

  std::atomic<int> done{0};
  std::atomic<int> not_found{0};
  auto cb = [&](Status status) {
    if (status.IsNotFound()) {
      not_found.fetch_add(1, std::memory_order_relaxed);
    }
    done.fetch_add(1, std::memory_order_relaxed);
  };

  std::string out(8, '\0');
  IOBuffer range_buffer(out.data(), out.size());
  RangeOption range_option;
  range_option.block_whole_length = 4096;

  cache.AsyncPut(Handle(), Buffer("remote-block"), cb, {});
  cache.AsyncRange(Handle(), 0, out.size(), &range_buffer, cb, range_option);
  cache.AsyncCache(Handle(), Buffer("remote-block"), cb, {});
  cache.AsyncPrefetch(Handle(), 4096, cb, {});

  ASSERT_TRUE(
      WaitUntil([&]() { return done.load(std::memory_order_relaxed) == 4; }));
  EXPECT_EQ(not_found.load(std::memory_order_relaxed), 4);

  cache.Shutdown();
}

TEST(RemoteBlockCacheMetricsTest, HitRatio) {
  RemoteBlockCacheMetrics vars;
  EXPECT_EQ(RemoteBlockCacheMetrics::GetCacheHitRatio(&vars), 0.0);
}

}  // namespace cache
}  // namespace dingofs
