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

#include "cache/remote/remote_cache_cluster.h"

#include <gtest/gtest.h>
#include <json/value.h>

#include <string>

#include "common/block/block_handle.h"
#include "common/block/block_key.h"
#include "common/io_buffer.h"

namespace dingofs {
namespace cache {

namespace {

BlockHandle Handle(uint64_t id) {
  return BlockHandle(1, BlockKey(id, 0, 4096));
}

IOBuffer Buffer(const std::string& data) {
  return IOBuffer(data.data(), data.size());
}

}  // namespace

TEST(RemoteCacheClusterTest, DumpEmptyGroup) {
  RemoteCacheCluster cluster;
  Json::Value value;

  EXPECT_TRUE(cluster.Dump(value));
  ASSERT_TRUE(value.isMember("members"));
  EXPECT_TRUE(value["members"].isArray());
  EXPECT_EQ(value["members"].size(), 0u);
}

TEST(RemoteCacheClusterTest, RequestsReturnNotFoundWithoutNode) {
  RemoteCacheCluster cluster;
  const std::string data = "remote-cache-payload";
  auto payload = Buffer(data);

  EXPECT_TRUE(cluster.SendPutRequest(Handle(1001), payload).IsNotFound());
  EXPECT_TRUE(cluster.SendCacheRequest(Handle(1002), payload).IsNotFound());
  EXPECT_TRUE(cluster.SendPrefetchRequest(Handle(1003), 4096).IsNotFound());

  std::string out(8, '\0');
  IOBuffer out_buffer(out.data(), out.size());
  bool cache_hit = true;
  EXPECT_TRUE(cluster
                  .SendRangeRequest(Handle(1004), 0, out.size(), &out_buffer,
                                    4096, &cache_hit)
                  .IsNotFound());
  EXPECT_TRUE(cache_hit);
}

TEST(RemoteCacheClusterMetricsGuardTest, RecordsSuccessAndErrors) {
  RemoteCacheClusterMetrics vars;

  {
    Status status = Status::OK();
    RemoteCacheClusterMetricsGuard guard("Put", 4096, status, &vars);
  }
  {
    Status status = Status::NetError("network");
    RemoteCacheClusterMetricsGuard guard("Range", 128, status, &vars);
  }
  {
    Status status = Status::OK();
    RemoteCacheClusterMetricsGuard guard("Cache", 4096, status, &vars);
  }
  {
    Status status = Status::NotFound("no node");
    RemoteCacheClusterMetricsGuard guard("Prefetch", 4096, status, &vars);
  }
}

}  // namespace cache
}  // namespace dingofs
