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
 * Created Date: 2026-09-24
 * Author: AI
 */

#ifndef DINGOFS_TEST_UNIT_BLOCKCACHE_BASE_MOCK_MDS_SERVER_H_
#define DINGOFS_TEST_UNIT_BLOCKCACHE_BASE_MOCK_MDS_SERVER_H_

#include <brpc/closure_guard.h>
#include <brpc/server.h>
#include <gmock/gmock.h>
#include <google/protobuf/service.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

#include "dingofs/error.pb.h"
#include "dingofs/mds.pb.h"

namespace dingofs {
namespace blockcache {

// The mds rpcs MDSClientImpl uses; every action must run `done`.
class MockMDSService : public pb::mds::MDSService {
 public:
  MockMDSService() = default;
  ~MockMDSService() override = default;

  MOCK_METHOD(void, Echo,
              (google::protobuf::RpcController * controller,
               const pb::mds::EchoRequest* request,
               pb::mds::EchoResponse* response,
               google::protobuf::Closure* done),
              (override));
  MOCK_METHOD(void, GetMDSList,
              (google::protobuf::RpcController * controller,
               const pb::mds::GetMDSListRequest* request,
               pb::mds::GetMDSListResponse* response,
               google::protobuf::Closure* done),
              (override));
  MOCK_METHOD(void, GetFsInfo,
              (google::protobuf::RpcController * controller,
               const pb::mds::GetFsInfoRequest* request,
               pb::mds::GetFsInfoResponse* response,
               google::protobuf::Closure* done),
              (override));
  MOCK_METHOD(void, JoinCacheGroup,
              (google::protobuf::RpcController * controller,
               const pb::mds::JoinCacheGroupRequest* request,
               pb::mds::JoinCacheGroupResponse* response,
               google::protobuf::Closure* done),
              (override));
  MOCK_METHOD(void, LeaveCacheGroup,
              (google::protobuf::RpcController * controller,
               const pb::mds::LeaveCacheGroupRequest* request,
               pb::mds::LeaveCacheGroupResponse* response,
               google::protobuf::Closure* done),
              (override));
  MOCK_METHOD(void, Heartbeat,
              (google::protobuf::RpcController * controller,
               const pb::mds::HeartbeatRequest* request,
               pb::mds::HeartbeatResponse* response,
               google::protobuf::Closure* done),
              (override));
  MOCK_METHOD(void, ListMembers,
              (google::protobuf::RpcController * controller,
               const pb::mds::ListMembersRequest* request,
               pb::mds::ListMembersResponse* response,
               google::protobuf::Closure* done),
              (override));
};

// Replies with `code`.
inline auto Reply(pb::error::Errno code, const std::string& message = "") {
  return [code, message](google::protobuf::RpcController*, const auto*,
                         auto* response, google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    response->mutable_error()->set_errcode(code);
    response->mutable_error()->set_errmsg(message);
  };
}

// Replies with `code` after `delay`.
inline auto ReplyAfter(std::chrono::milliseconds delay, pb::error::Errno code) {
  return [delay, code](google::protobuf::RpcController*, const auto*,
                       auto* response, google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::this_thread::sleep_for(delay);
    response->mutable_error()->set_errcode(code);
  };
}

// Runs `action` and counts the call.
template <typename Action>
auto Counting(std::atomic<int>* calls, Action action) {
  return [calls, action](auto* controller, auto* request, auto* response,
                         auto* done) {
    ++*calls;
    action(controller, request, response, done);
  };
}

// The default reply: ENOT_SUPPORT is not retried, so the call fails fast.
inline auto Unexpected() {
  return Reply(pb::error::ENOT_SUPPORT, "no expectation for this call");
}

// A mds server on a random local port, backed by MockMDSService.
class MockMDSServer {
 public:
  MockMDSServer() {
    using ::testing::_;
    ON_CALL(service_, Echo(_, _, _, _)).WillByDefault(Reply(pb::error::OK));
    ON_CALL(service_, GetFsInfo(_, _, _, _)).WillByDefault(Unexpected());
    ON_CALL(service_, JoinCacheGroup(_, _, _, _)).WillByDefault(Unexpected());
    ON_CALL(service_, LeaveCacheGroup(_, _, _, _)).WillByDefault(Unexpected());
    ON_CALL(service_, Heartbeat(_, _, _, _)).WillByDefault(Unexpected());
    ON_CALL(service_, ListMembers(_, _, _, _)).WillByDefault(Unexpected());
    ServeMDSList({});
  }

  ~MockMDSServer() { Stop(); }

  MockMDSServer(const MockMDSServer&) = delete;
  MockMDSServer& operator=(const MockMDSServer&) = delete;

  void Start() {
    ASSERT_EQ(server_.AddService(&service_, brpc::SERVER_DOESNT_OWN_SERVICE),
              0);
    brpc::ServerOptions options;
    ASSERT_EQ(server_.Start("127.0.0.1:0", &options), 0);
    port_ = server_.listen_address().port;
  }

  void Stop() {
    server_.Stop(0);
    server_.Join();
  }

  // This server as a mds list entry.
  pb::mds::MDS Entry(int64_t id) const {
    pb::mds::MDS mds;
    mds.set_id(id);
    mds.mutable_location()->set_host("127.0.0.1");
    mds.mutable_location()->set_port(port_);
    mds.set_state(pb::mds::MDS_State_NORMAL);
    mds.set_create_time_ms(1758672000000);
    mds.set_last_online_time_ms(1758672000000);
    mds.set_is_online(true);
    return mds;
  }

  // Sets the GetMDSList reply; every call is counted.
  void ServeMDSList(const std::vector<pb::mds::MDS>& mdses) {
    using ::testing::_;
    ON_CALL(service_, GetMDSList(_, _, _, _))
        .WillByDefault([this, mdses](google::protobuf::RpcController*,
                                     const pb::mds::GetMDSListRequest*,
                                     pb::mds::GetMDSListResponse* response,
                                     google::protobuf::Closure* done) {
          brpc::ClosureGuard done_guard(done);
          mds_list_calls_++;
          for (const auto& mds : mdses) {
            *response->add_mdses() = mds;
          }
        });
  }

  int port() const { return port_; }
  std::string addr() const { return "127.0.0.1:" + std::to_string(port_); }
  ::testing::NiceMock<MockMDSService>& service() { return service_; }
  int mds_list_calls() const { return mds_list_calls_.load(); }

 private:
  ::testing::NiceMock<MockMDSService> service_;
  brpc::Server server_;  // after service_: torn down first
  int port_ = 0;
  std::atomic<int> mds_list_calls_{0};
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_TEST_UNIT_BLOCKCACHE_BASE_MOCK_MDS_SERVER_H_
