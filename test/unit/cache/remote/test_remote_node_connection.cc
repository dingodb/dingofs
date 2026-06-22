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

#include "cache/remote/remote_node_connection.h"

#include <gtest/gtest.h>

#include <cerrno>
#include <memory>
#include <string>

#include "common/options/cache.h"
#include "dingofs/blockcache.pb.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {

class RemoteNodeConnectionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    saved_use_rdma_ = FLAGS_use_rdma;
    FLAGS_use_rdma = false;
  }

  void TearDown() override { FLAGS_use_rdma = saved_use_rdma_; }

 private:
  bool saved_use_rdma_{false};
};

TEST_F(RemoteNodeConnectionTest, ResultSetFailed) {
  RemoteNodeConnection::Result result;
  result.SetFailed(ETIMEDOUT, "timeout", true);

  EXPECT_TRUE(result.failed);
  EXPECT_EQ(result.error_code, ETIMEDOUT);
  EXPECT_EQ(result.error_text, "timeout");
  EXPECT_TRUE(result.conn_broken);
}

TEST_F(RemoteNodeConnectionTest, NewReturnsTcpWhenRdmaDisabled) {
  auto conn = RemoteNodeConnection::New();
  ASSERT_NE(conn, nullptr);
  EXPECT_NE(dynamic_cast<TCPConnection*>(conn.get()), nullptr);
}

TEST_F(RemoteNodeConnectionTest, NewReturnsRdmaWhenEnabled) {
  FLAGS_use_rdma = true;

  auto conn = RemoteNodeConnection::New();
  ASSERT_NE(conn, nullptr);
  EXPECT_NE(dynamic_cast<RDMAConnection*>(conn.get()), nullptr);
}

TEST_F(RemoteNodeConnectionTest, TcpConnectRejectsBadEndpoint) {
  TCPConnection conn;
  EXPECT_FALSE(conn.IsConnected());

  EXPECT_TRUE(conn.Connect("not-an-ip", 9999, 1).IsInternal());
  EXPECT_FALSE(conn.IsConnected());
}

TEST_F(RemoteNodeConnectionTest, TcpConnectAndClose) {
  TCPConnection conn;

  ASSERT_TRUE(conn.Connect("127.0.0.1", 9, 1).ok());
  EXPECT_TRUE(conn.IsConnected());
  EXPECT_TRUE(conn.Connect("127.0.0.1", 9, 1).ok());

  conn.Close();
  EXPECT_FALSE(conn.IsConnected());
}

TEST_F(RemoteNodeConnectionTest, TcpSendWithoutConnectionFails) {
  TCPConnection conn;
  pb::cache::PingRequest request;
  pb::cache::PingResponse response;
  RemoteNodeConnection::Result result;

  conn.Send("Ping", request, &response, nullptr, nullptr, 1, &result);

  EXPECT_TRUE(result.failed);
  EXPECT_EQ(result.error_code, EIO);
  EXPECT_NE(result.error_text.find("not connected"), std::string::npos);
  EXPECT_TRUE(result.conn_broken);
}

TEST_F(RemoteNodeConnectionTest, TcpSendConnectedFailureMarksConnectionBroken) {
  TCPConnection conn;
  ASSERT_TRUE(conn.Connect("127.0.0.1", 9, 1).ok());

  pb::cache::PutRequest request;
  pb::cache::PutResponse response;
  RemoteNodeConnection::Result result;

  conn.Send("Put", request, &response, nullptr, nullptr, 1, &result);

  EXPECT_TRUE(result.failed);
  EXPECT_NE(result.error_code, 0);
  EXPECT_FALSE(result.error_text.empty());
  EXPECT_TRUE(result.conn_broken);
}

TEST_F(RemoteNodeConnectionTest, RdmaSendWithoutConnectionFails) {
  RDMAConnection conn;
  pb::cache::PingRequest request;
  pb::cache::PingResponse response;
  RemoteNodeConnection::Result result;

  EXPECT_FALSE(conn.IsConnected());
  conn.Close();
  conn.Send("Ping", request, &response, nullptr, nullptr, 1, &result);

  EXPECT_TRUE(result.failed);
  EXPECT_EQ(result.error_code, pb::infiniband::ErrorCode::InternalError);
  EXPECT_NE(result.error_text.find("not connected"), std::string::npos);
  EXPECT_TRUE(result.conn_broken);
}

}  // namespace cache
}  // namespace dingofs
