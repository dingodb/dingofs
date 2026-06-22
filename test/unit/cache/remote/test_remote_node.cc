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

#include <sstream>
#include <string>

#include "cache/common/block_handle_helper.h"
#include "cache/remote/remote_node.h"
#include "common/options/cache.h"
#include "dingofs/blockcache.pb.h"

namespace dingofs {
namespace cache {

namespace {

pb::cache::BlockHandle RawHandle() {
  return ToHandlePB(BlockHandle(1, BlockKey(100, 0, 4096)));
}

template <typename T>
Request<T> MakeRawRequest(const std::string& method) {
  T raw;
  *raw.mutable_handle() = RawHandle();
  return MakeRequest(method, raw);
}

}  // namespace

class RemoteNodeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    saved_connections_ = FLAGS_connections;
    saved_connect_timeout_ms_ = FLAGS_cache_rpc_connect_timeout_ms;
    saved_check_duration_ms_ = FLAGS_cache_node_state_check_duration_ms;
    saved_put_timeout_ms_ = FLAGS_cache_put_rpc_timeout_ms;
    saved_range_timeout_ms_ = FLAGS_cache_range_rpc_timeout_ms;
    saved_max_retry_times_ = FLAGS_cache_rpc_max_retry_times;
    saved_max_timeout_ms_ = FLAGS_cache_rpc_max_timeout_ms;

    FLAGS_connections = 1;
    FLAGS_cache_rpc_connect_timeout_ms = 1;
    FLAGS_cache_node_state_check_duration_ms = 60000;
    FLAGS_cache_put_rpc_timeout_ms = 1;
    FLAGS_cache_range_rpc_timeout_ms = 1;
    FLAGS_cache_rpc_max_retry_times = 1;
    FLAGS_cache_rpc_max_timeout_ms = 1;
  }

  void TearDown() override {
    FLAGS_connections = saved_connections_;
    FLAGS_cache_rpc_connect_timeout_ms = saved_connect_timeout_ms_;
    FLAGS_cache_node_state_check_duration_ms = saved_check_duration_ms_;
    FLAGS_cache_put_rpc_timeout_ms = saved_put_timeout_ms_;
    FLAGS_cache_range_rpc_timeout_ms = saved_range_timeout_ms_;
    FLAGS_cache_rpc_max_retry_times = saved_max_retry_times_;
    FLAGS_cache_rpc_max_timeout_ms = saved_max_timeout_ms_;
  }

 private:
  int32_t saved_connections_{0};
  uint32_t saved_connect_timeout_ms_{0};
  uint32_t saved_check_duration_ms_{0};
  uint32_t saved_put_timeout_ms_{0};
  uint32_t saved_range_timeout_ms_{0};
  uint32_t saved_max_retry_times_{0};
  uint32_t saved_max_timeout_ms_{0};
};

TEST_F(RemoteNodeTest, AccessorsDumpAndStream) {
  RemoteNode node("044d4698-7bd4-4e44-9e94-aee6312ff06f", "10.0.1.8", 9300, 20);

  EXPECT_EQ(node.Id(), "044d4698-7bd4-4e44-9e94-aee6312ff06f");
  EXPECT_EQ(node.IP(), "10.0.1.8");
  EXPECT_EQ(node.Port(), 9300);
  EXPECT_EQ(node.Weight(), 20);
  EXPECT_TRUE(node.IsHealthy());

  Json::Value value;
  EXPECT_TRUE(node.Dump(value));
  EXPECT_EQ(value["id"].asString(), node.Id());
  EXPECT_EQ(value["endpoint"].asString(), "10.0.1.8:9300");
  EXPECT_EQ(value["weight"].asUInt(), 20u);
  EXPECT_EQ(value["connections"].asInt(), 1);
  EXPECT_TRUE(value["healthy"].asBool());

  std::ostringstream oss;
  oss << node;
  EXPECT_NE(oss.str().find("RemoteNode{id=044d4698"), std::string::npos);
  EXPECT_NE(oss.str().find("conns=1"), std::string::npos);
}

TEST_F(RemoteNodeTest, StartToleratesDisconnectedConnections) {
  RemoteNode node("20f2fc27-2f29-4975-8d08-836ec63b8f91", "not-an-ip", 9300,
                  10);

  EXPECT_TRUE(node.Start().ok());
  EXPECT_TRUE(node.Start().ok());
  EXPECT_TRUE(node.IsHealthy());

  node.Shutdown();
}

TEST_F(RemoteNodeTest, SendPutReturnsNetErrorWhenConnectedRpcFails) {
  RemoteNode node("20f2fc27-2f29-4975-8d08-836ec63b8f92", "127.0.0.1", 9, 10);
  ASSERT_TRUE(node.Start().ok());

  auto request = MakeRawRequest<pb::cache::PutRequest>("Put");
  auto response =
      node.SendRequest<pb::cache::PutRequest, pb::cache::PutResponse>(request);

  EXPECT_TRUE(response.status.IsNetError());
  node.Shutdown();
}

TEST_F(RemoteNodeTest, SendRangeRetriesThenReturnsInternalWhenRpcKeepsFailing) {
  RemoteNode node("20f2fc27-2f29-4975-8d08-836ec63b8f93", "127.0.0.1", 9, 10);
  ASSERT_TRUE(node.Start().ok());

  auto request = MakeRawRequest<pb::cache::RangeRequest>("Range");
  auto response =
      node.SendRequest<pb::cache::RangeRequest, pb::cache::RangeResponse>(
          request);

  EXPECT_TRUE(response.status.IsInternal());
  node.Shutdown();
}

}  // namespace cache
}  // namespace dingofs
