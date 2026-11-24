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

#include <brpc/closure_guard.h>
#include <gflags/gflags.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "blockcache/common/mds_client.h"
#include "common/options/cache.h"
#include "common/status.h"
#include "dingofs/error.pb.h"
#include "dingofs/mds.pb.h"
#include "test/unit/blockcache/base/mock_mds_server.h"

namespace dingofs {
namespace blockcache {

using ::testing::_;

constexpr char kMemberId[] = "044d4698-7bd4-4e44-9e94-aee6312ff06f";
constexpr char kMemberIp[] = "10.0.0.12";
constexpr uint32_t kMemberPort = 10001;
constexpr uint32_t kWeight = 100;
constexpr char kGroupName[] = "cache-group-01";
constexpr uint64_t kFsId = 10001;
constexpr int64_t kMdsId = 1001;

static CacheGroupMember Member() {
  return {.id = kMemberId,
          .ip = kMemberIp,
          .port = kMemberPort,
          .weight = kWeight,
          .state = CacheGroupMemberState::kOnline};
}

template <typename Fn>
static uint64_t ElapsedUs(Fn fn) {
  const auto start = std::chrono::steady_clock::now();
  fn();
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::steady_clock::now() - start)
      .count();
}

// Saves the mds flags a test may change.
struct MDSFlags {
  std::string addrs = FLAGS_mds_addrs;
  int64_t rpc_timeout_ms = FLAGS_mds_rpc_timeout_ms;

  void Restore() const {
    FLAGS_mds_addrs = addrs;
    FLAGS_mds_rpc_timeout_ms = rpc_timeout_ms;
  }
};

TEST(CacheGroupMemberTest, StateToString) {
  EXPECT_EQ(CacheGroupMemberStateToString(CacheGroupMemberState::kUnknown),
            "unknown");
  EXPECT_EQ(CacheGroupMemberStateToString(CacheGroupMemberState::kOnline),
            "online");
  EXPECT_EQ(CacheGroupMemberStateToString(CacheGroupMemberState::kUnstable),
            "unstable");
  EXPECT_EQ(CacheGroupMemberStateToString(CacheGroupMemberState::kOffline),
            "offline");
}

TEST(CacheGroupMemberTest, Equality) {
  const CacheGroupMember member = Member();
  EXPECT_TRUE(member == Member());

  {  // every field counts
    CacheGroupMember other = Member();
    other.id = "1b6c9f0e-2a37-4d8e-9c41-5e7f0a2b3c4d";
    EXPECT_FALSE(member == other);

    other = Member();
    other.ip = "10.0.0.13";
    EXPECT_FALSE(member == other);

    other = Member();
    other.port = 10002;
    EXPECT_FALSE(member == other);

    other = Member();
    other.weight = 50;
    EXPECT_FALSE(member == other);

    other = Member();
    other.state = CacheGroupMemberState::kUnstable;
    EXPECT_FALSE(member == other);
  }
}

TEST(CacheGroupMemberTest, Print) {
  std::ostringstream os;
  os << Member();
  EXPECT_EQ(os.str(),
            "CacheGroupMember{id=044d4698-7bd4-4e44-9e94-aee6312ff06f "
            "ip=10.0.0.12 port=10001 weight=100 state=online}");
}

class MDSClientTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_NO_FATAL_FAILURE(mds_server_.Start());
    mds_server_.ServeMDSList({mds_server_.Entry(kMdsId)});

    FLAGS_mds_addrs = mds_server_.addr();
    FLAGS_mds_rpc_timeout_ms = 1000;
    mds_client_ = std::make_unique<MDSClientImpl>();  // reads FLAGS_mds_addrs
  }

  void TearDown() override {
    if (mds_client_ != nullptr) {
      mds_client_->Shutdown();
      mds_client_.reset();
    }
    saved_flags_.Restore();
  }

  MDSFlags saved_flags_;
  MockMDSServer mds_server_;
  ::testing::NiceMock<MockMDSService>& mds_service_ = mds_server_.service();
  MDSClientUPtr mds_client_;
};

TEST_F(MDSClientTest, StartAndShutdown) {
  std::atomic<int> echo_calls{0};
  EXPECT_CALL(mds_service_, Echo(_, _, _, _))
      .WillRepeatedly(Counting(&echo_calls, Reply(pb::error::OK)));

  {  // starting probes the mds and fetches the mds list
    ASSERT_TRUE(mds_client_->Start().ok());
    EXPECT_EQ(echo_calls, 1);
    EXPECT_EQ(mds_server_.mds_list_calls(), 1);
  }

  {  // a second Start() is a no-op
    EXPECT_TRUE(mds_client_->Start().ok());
    EXPECT_EQ(echo_calls, 1);
    EXPECT_EQ(mds_server_.mds_list_calls(), 1);
  }

  {  // requests work while running
    EXPECT_CALL(mds_service_, Heartbeat(_, _, _, _))
        .WillOnce(Reply(pb::error::OK));
    EXPECT_TRUE(mds_client_->Heartbeat(kMemberId, kMemberIp, kMemberPort).ok());
  }

  {  // Shutdown() twice is fine
    mds_client_->Shutdown();
    mds_client_->Shutdown();
  }

  // a stopped client cannot start again, MDSDiscovery can't start while stopped
  {
    EXPECT_TRUE(mds_client_->Start().IsInternal());
    EXPECT_EQ(echo_calls, 2);
    EXPECT_EQ(mds_server_.mds_list_calls(), 1);
  }
}

TEST_F(MDSClientTest, StartFailure) {
  {  // nobody listens on the address
    FLAGS_mds_addrs = "127.0.0.1:1";
    MDSClientImpl client;
    EXPECT_TRUE(client.Start().IsInternal());
    client.Shutdown();  // never ran: nothing to stop
  }

  {  // a dead address is fine if another one answers
    FLAGS_mds_addrs = "127.0.0.1:1," + mds_server_.addr();
    MDSClientImpl client;
    EXPECT_TRUE(client.Start().ok());
    client.Shutdown();
  }

  {  // an empty mds list: Start() retries until Shutdown()
    mds_server_.ServeMDSList({});
    Status status;
    std::thread starter([&]() { status = mds_client_->Start(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    EXPECT_GE(mds_server_.mds_list_calls(), 2);
    mds_client_->Shutdown();
    starter.join();
    EXPECT_TRUE(status.IsInternal()) << status.ToString();
  }
}

TEST_F(MDSClientTest, GetFSInfo) {
  ASSERT_TRUE(mds_client_->Start().ok());

  {  // the fs info comes back as the mds has it
    EXPECT_CALL(mds_service_, GetFsInfo(_, _, _, _))
        .WillOnce([](auto*, const pb::mds::GetFsInfoRequest* request,
                     pb::mds::GetFsInfoResponse* response, auto* done) {
          brpc::ClosureGuard done_guard(done);
          EXPECT_EQ(request->fs_id(), kFsId);
          auto* fs_info = response->mutable_fs_info();
          fs_info->set_fs_id(kFsId);
          fs_info->set_fs_name("dingofs-cache-01");
          fs_info->set_fs_type(pb::mds::FsType::LOCALFILE);
          fs_info->mutable_extra()->mutable_file_info()->set_path(
              "/mnt/dingofs-cache-01");
        });

    pb::mds::FsInfo fs_info;
    ASSERT_TRUE(mds_client_->GetFSInfo(kFsId, &fs_info).ok());
    EXPECT_EQ(fs_info.fs_id(), kFsId);
    EXPECT_EQ(fs_info.fs_name(), "dingofs-cache-01");
    EXPECT_EQ(fs_info.fs_type(), pb::mds::FsType::LOCALFILE);
    EXPECT_EQ(fs_info.extra().file_info().path(), "/mnt/dingofs-cache-01");
  }

  {  // an unknown fs leaves the output alone
    EXPECT_CALL(mds_service_, GetFsInfo(_, _, _, _))
        .WillOnce(Reply(pb::error::ENOT_FOUND, "fs not found"));

    pb::mds::FsInfo fs_info;
    fs_info.set_fs_id(10002);
    const Status status = mds_client_->GetFSInfo(10002, &fs_info);
    EXPECT_TRUE(status.IsNotExist()) << status.ToString();
    EXPECT_EQ(status.ToString(), "NotExist (errno:10003) : fs not found");
    EXPECT_EQ(fs_info.fs_id(), 10002u);
    EXPECT_TRUE(fs_info.fs_name().empty());
  }

  {  // a retriable error is retried, a persistent one fails
    EXPECT_CALL(mds_service_, GetFsInfo(_, _, _, _))
        .WillOnce(Reply(pb::error::ENOT_SERVE, "not serving"))
        .WillOnce(Reply(pb::error::OK));
    pb::mds::FsInfo fs_info;
    EXPECT_TRUE(mds_client_->GetFSInfo(kFsId, &fs_info).ok());

    EXPECT_CALL(mds_service_, GetFsInfo(_, _, _, _))
        .Times(6)
        .WillRepeatedly(Reply(pb::error::EINTERNAL, "store unavailable"));
    EXPECT_TRUE(mds_client_->GetFSInfo(kFsId, &fs_info).IsInternal());
  }
}

TEST_F(MDSClientTest, JoinCacheGroup) {
  ASSERT_TRUE(mds_client_->Start().ok());

  {  // the member's identity and weight reach the mds as given
    EXPECT_CALL(mds_service_, JoinCacheGroup(_, _, _, _))
        .WillOnce([](auto*, const pb::mds::JoinCacheGroupRequest* request,
                     pb::mds::JoinCacheGroupResponse*, auto* done) {
          brpc::ClosureGuard done_guard(done);
          EXPECT_EQ(request->member_id(), kMemberId);
          EXPECT_EQ(request->ip(), kMemberIp);
          EXPECT_EQ(request->port(), kMemberPort);
          EXPECT_EQ(request->group_name(), kGroupName);
          EXPECT_EQ(request->weight(), kWeight);
        });
    EXPECT_TRUE(mds_client_
                    ->JoinCacheGroup(kMemberId, kMemberIp, kMemberPort,
                                     kGroupName, kWeight)
                    .ok());
  }

  {  // the mds refuses, e.g. the id is taken by another ip:port
    EXPECT_CALL(mds_service_, JoinCacheGroup(_, _, _, _))
        .WillOnce(Reply(pb::error::EILLEGAL_PARAMTETER, "member id locked"));
    EXPECT_TRUE(mds_client_
                    ->JoinCacheGroup(kMemberId, "10.0.0.13", kMemberPort,
                                     kGroupName, kWeight)
                    .IsInvalidParam());
  }

  {  // a retriable error is retried, a persistent one fails
    EXPECT_CALL(mds_service_, JoinCacheGroup(_, _, _, _))
        .WillOnce(Reply(pb::error::ENOT_SERVE, "not serving"))
        .WillOnce(Reply(pb::error::OK));
    EXPECT_TRUE(mds_client_
                    ->JoinCacheGroup(kMemberId, kMemberIp, kMemberPort,
                                     kGroupName, kWeight)
                    .ok());

    EXPECT_CALL(mds_service_, JoinCacheGroup(_, _, _, _))
        .Times(6)
        .WillRepeatedly(Reply(pb::error::EINTERNAL, "store unavailable"));
    EXPECT_TRUE(mds_client_
                    ->JoinCacheGroup(kMemberId, kMemberIp, kMemberPort,
                                     kGroupName, kWeight)
                    .IsInternal());
  }
}

TEST_F(MDSClientTest, LeaveCacheGroup) {
  ASSERT_TRUE(mds_client_->Start().ok());

  {  // the member's identity reaches the mds as given
    EXPECT_CALL(mds_service_, LeaveCacheGroup(_, _, _, _))
        .WillOnce([](auto*, const pb::mds::LeaveCacheGroupRequest* request,
                     pb::mds::LeaveCacheGroupResponse*, auto* done) {
          brpc::ClosureGuard done_guard(done);
          EXPECT_EQ(request->member_id(), kMemberId);
          EXPECT_EQ(request->ip(), kMemberIp);
          EXPECT_EQ(request->port(), kMemberPort);
          EXPECT_EQ(request->group_name(), kGroupName);
        });
    EXPECT_TRUE(
        mds_client_
            ->LeaveCacheGroup(kMemberId, kMemberIp, kMemberPort, kGroupName)
            .ok());
  }

  {  // the mds knows no such member
    EXPECT_CALL(mds_service_, LeaveCacheGroup(_, _, _, _))
        .WillOnce(Reply(pb::error::ENOT_FOUND, "member not found"));
    EXPECT_TRUE(
        mds_client_
            ->LeaveCacheGroup(kMemberId, kMemberIp, kMemberPort, kGroupName)
            .IsNotExist());
  }

  {  // a retriable error is retried, a persistent one fails
    EXPECT_CALL(mds_service_, LeaveCacheGroup(_, _, _, _))
        .WillOnce(Reply(pb::error::ENOT_SERVE, "not serving"))
        .WillOnce(Reply(pb::error::OK));
    EXPECT_TRUE(
        mds_client_
            ->LeaveCacheGroup(kMemberId, kMemberIp, kMemberPort, kGroupName)
            .ok());

    EXPECT_CALL(mds_service_, LeaveCacheGroup(_, _, _, _))
        .Times(6)
        .WillRepeatedly(Reply(pb::error::EINTERNAL, "store unavailable"));
    EXPECT_TRUE(
        mds_client_
            ->LeaveCacheGroup(kMemberId, kMemberIp, kMemberPort, kGroupName)
            .IsInternal());
  }
}

TEST_F(MDSClientTest, Heartbeat) {
  ASSERT_TRUE(mds_client_->Start().ok());

  {  // sent as a cache member, with its address
    EXPECT_CALL(mds_service_, Heartbeat(_, _, _, _))
        .WillOnce([](auto*, const pb::mds::HeartbeatRequest* request,
                     pb::mds::HeartbeatResponse*, auto* done) {
          brpc::ClosureGuard done_guard(done);
          EXPECT_EQ(request->role(), pb::mds::ROLE_CACHE_MEMBER);
          ASSERT_TRUE(request->has_cache_group_member());
          EXPECT_EQ(request->cache_group_member().member_id(), kMemberId);
          EXPECT_EQ(request->cache_group_member().ip(), kMemberIp);
          EXPECT_EQ(request->cache_group_member().port(), kMemberPort);
        });
    EXPECT_TRUE(mds_client_->Heartbeat(kMemberId, kMemberIp, kMemberPort).ok());
  }

  {  // gives up after 3 tries, each sent twice by RPC::SendRequest
    std::atomic<int> calls{0};
    EXPECT_CALL(mds_service_, Heartbeat(_, _, _, _))
        .Times(6)
        .WillRepeatedly(
            Counting(&calls, Reply(pb::error::EINTERNAL, "store unavailable")));
    const Status status =
        mds_client_->Heartbeat(kMemberId, kMemberIp, kMemberPort);
    EXPECT_TRUE(status.IsInternal()) << status.ToString();
    EXPECT_EQ(status.ToString(), "Internal: send rpc request fail");
    EXPECT_EQ(calls, 6);
  }
}

TEST_F(MDSClientTest, ListMembers) {
  ASSERT_TRUE(mds_client_->Start().ok());

  {  // every member of the group, in the state the mds reports
    EXPECT_CALL(mds_service_, ListMembers(_, _, _, _))
        .WillOnce([](auto*, const pb::mds::ListMembersRequest* request,
                     pb::mds::ListMembersResponse* response, auto* done) {
          brpc::ClosureGuard done_guard(done);
          EXPECT_EQ(request->group_name(), kGroupName);
          struct Entry {
            const char* id;
            const char* ip;
            uint32_t port;
            uint32_t weight;
            pb::mds::CacheGroupMemberState state;
          };
          const Entry entries[] = {
              {.id = kMemberId,
               .ip = kMemberIp,
               .port = kMemberPort,
               .weight = kWeight,
               .state = pb::mds::CacheGroupMemberStateOnline},
              {.id = "1b6c9f0e-2a37-4d8e-9c41-5e7f0a2b3c4d",
               .ip = "10.0.0.13",
               .port = 10001,
               .weight = 50,
               .state = pb::mds::CacheGroupMemberStateUnstable},
              {.id = "7c2d1e4a-9b8f-4a6c-8d3e-2f1a0b9c8d7e",
               .ip = "10.0.0.14",
               .port = 10002,
               .weight = 100,
               .state = pb::mds::CacheGroupMemberStateOffline},
              {.id = "e3f4a5b6-c7d8-4e9f-a0b1-c2d3e4f5a6b7",
               .ip = "10.0.0.15",
               .port = 10001,
               .weight = 100,
               .state = pb::mds::CacheGroupMemberStateUnknown},
              // an unknown state
              {.id = "9a8b7c6d-5e4f-4a3b-9c2d-1e0f9a8b7c6d",
               .ip = "10.0.0.16",
               .port = 10001,
               .weight = 100,
               .state = static_cast<pb::mds::CacheGroupMemberState>(7)},
          };
          for (const Entry& entry : entries) {
            auto* member = response->add_members();
            member->set_member_id(entry.id);
            member->set_ip(entry.ip);
            member->set_port(entry.port);
            member->set_weight(entry.weight);
            member->set_state(entry.state);
            member->set_group_name(kGroupName);
          }
        });

    // members are appended to what the list holds
    std::vector<CacheGroupMember> members = {CacheGroupMember{
        .id = "stale", .state = CacheGroupMemberState::kOffline}};
    ASSERT_TRUE(mds_client_->ListMembers(kGroupName, &members).ok());
    ASSERT_EQ(members.size(), 6u);
    EXPECT_EQ(members[0].id, "stale");
    EXPECT_EQ(members[1], Member());
    EXPECT_EQ(members[2],
              (CacheGroupMember{.id = "1b6c9f0e-2a37-4d8e-9c41-5e7f0a2b3c4d",
                                .ip = "10.0.0.13",
                                .port = 10001,
                                .weight = 50,
                                .state = CacheGroupMemberState::kUnstable}));
    EXPECT_EQ(members[3].ip, "10.0.0.14");
    EXPECT_EQ(members[3].port, 10002u);
    EXPECT_EQ(members[3].state, CacheGroupMemberState::kOffline);
    EXPECT_EQ(members[4].state, CacheGroupMemberState::kUnknown);
    EXPECT_EQ(members[5].state, CacheGroupMemberState::kUnknown);
  }

  {  // no such group: the list is left alone
    EXPECT_CALL(mds_service_, ListMembers(_, _, _, _))
        .WillOnce(Reply(pb::error::ENOT_FOUND, "group not found"));
    std::vector<CacheGroupMember> members;
    EXPECT_TRUE(
        mds_client_->ListMembers("no-such-group", &members).IsNotExist());
    EXPECT_TRUE(members.empty());
  }

  {  // a retriable error is retried, a persistent one fails
    EXPECT_CALL(mds_service_, ListMembers(_, _, _, _))
        .WillOnce(Reply(pb::error::ENOT_SERVE, "not serving"))
        .WillOnce(Reply(pb::error::OK));
    std::vector<CacheGroupMember> members;
    EXPECT_TRUE(mds_client_->ListMembers(kGroupName, &members).ok());
    EXPECT_TRUE(members.empty());

    EXPECT_CALL(mds_service_, ListMembers(_, _, _, _))
        .Times(6)
        .WillRepeatedly(Reply(pb::error::EINTERNAL, "store unavailable"));
    EXPECT_TRUE(mds_client_->ListMembers(kGroupName, &members).IsInternal());
    EXPECT_TRUE(members.empty());
  }
}

TEST_F(MDSClientTest, Retry) {
  ASSERT_TRUE(mds_client_->Start().ok());

  {  // retried once, after refreshing the mds list
    for (const pb::error::Errno code :
         {pb::error::ENOT_SERVE, pb::error::EROUTER_EPOCH_CHANGE,
          pb::error::ENOT_CAN_CONNECTED}) {
      const std::string name = pb::error::Errno_Name(code);
      std::atomic<int> calls{0};
      EXPECT_CALL(mds_service_, Heartbeat(_, _, _, _))
          .Times(2)
          .WillOnce(Counting(&calls, Reply(code, "try again")))
          .WillOnce(Counting(&calls, Reply(pb::error::OK)));

      const int refreshes = mds_server_.mds_list_calls();
      const Status status =
          mds_client_->Heartbeat(kMemberId, kMemberIp, kMemberPort);
      EXPECT_TRUE(status.ok()) << name << ": " << status.ToString();
      EXPECT_EQ(calls, 2) << name;
      EXPECT_EQ(mds_server_.mds_list_calls() - refreshes, 1) << name;
    }
  }
}

TEST_F(MDSClientTest, Unretriable) {
  ASSERT_TRUE(mds_client_->Start().ok());

  // returned at once, without refreshing the mds list
  struct Case {
    pb::error::Errno code;
    bool (Status::*is)() const;
  };
  const Case cases[] = {
      {.code = pb::error::ENOT_FOUND, .is = &Status::IsNotExist},
      {.code = pb::error::EILLEGAL_PARAMTETER, .is = &Status::IsInvalidParam},
      {.code = pb::error::ENOT_SUPPORT, .is = &Status::IsNotSupport},
      {.code = pb::error::EEXISTED, .is = &Status::IsExist},
      {.code = pb::error::ENOT_EMPTY, .is = &Status::IsNotEmpty},
  };
  const int refreshes = mds_server_.mds_list_calls();
  for (const Case& c : cases) {
    const std::string name = pb::error::Errno_Name(c.code);
    std::atomic<int> calls{0};
    EXPECT_CALL(mds_service_, Heartbeat(_, _, _, _))
        .WillOnce(Counting(&calls, Reply(c.code, name)));

    const Status status =
        mds_client_->Heartbeat(kMemberId, kMemberIp, kMemberPort);
    EXPECT_TRUE((status.*c.is)()) << name << ": " << status.ToString();
    EXPECT_EQ(status.Errno(), static_cast<int32_t>(c.code)) << name;
    EXPECT_EQ(calls, 1) << name;
  }
  EXPECT_EQ(mds_server_.mds_list_calls(), refreshes);
}

TEST_F(MDSClientTest, RpcLevelRetry) {
  ASSERT_TRUE(mds_client_->Start().ok());

  // RPC::SendRequest retries EINTERNAL itself: no mds list refresh
  std::atomic<int> calls{0};
  EXPECT_CALL(mds_service_, Heartbeat(_, _, _, _))
      .Times(2)
      .WillOnce(Counting(&calls, Reply(pb::error::EINTERNAL, "try again")))
      .WillOnce(Counting(&calls, Reply(pb::error::OK)));

  const int refreshes = mds_server_.mds_list_calls();
  const Status status =
      mds_client_->Heartbeat(kMemberId, kMemberIp, kMemberPort);
  EXPECT_TRUE(status.ok()) << status.ToString();
  EXPECT_EQ(calls, 2);
  EXPECT_EQ(mds_server_.mds_list_calls(), refreshes);
}

TEST_F(MDSClientTest, Timeout) {
  ASSERT_TRUE(mds_client_->Start().ok());

  {  // every request carries the current timeout
    std::vector<uint32_t> timeouts;
    EXPECT_CALL(mds_service_, Heartbeat(_, _, _, _))
        .Times(2)
        .WillRepeatedly([&](auto*, const pb::mds::HeartbeatRequest* request,
                            auto*, auto* done) {
          brpc::ClosureGuard done_guard(done);
          timeouts.push_back(request->info().timeout_ms());
        });
    EXPECT_TRUE(mds_client_->Heartbeat(kMemberId, kMemberIp, kMemberPort).ok());
    FLAGS_mds_rpc_timeout_ms = 500;
    EXPECT_TRUE(mds_client_->Heartbeat(kMemberId, kMemberIp, kMemberPort).ok());
    EXPECT_EQ(timeouts, (std::vector<uint32_t>{1000, 500}));
  }

  {  // a slow reply is dropped and resent on the same mds
    FLAGS_mds_rpc_timeout_ms = 200;
    std::atomic<int> calls{0};
    EXPECT_CALL(mds_service_, Heartbeat(_, _, _, _))
        .Times(2)
        .WillOnce(Counting(
            &calls, ReplyAfter(std::chrono::milliseconds(800), pb::error::OK)))
        .WillOnce(Counting(&calls, Reply(pb::error::OK)));

    const int refreshes = mds_server_.mds_list_calls();
    Status status;
    const uint64_t elapsed_us = ElapsedUs([&]() {
      status = mds_client_->Heartbeat(kMemberId, kMemberIp, kMemberPort);
    });
    EXPECT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(calls, 2);
    EXPECT_EQ(mds_server_.mds_list_calls(), refreshes);
    EXPECT_GE(elapsed_us / 1000, 200);
    EXPECT_LT(elapsed_us / 1000, 800);
  }

  {  // timing out every time: 3 tries x 2 sends, then fails
    FLAGS_mds_rpc_timeout_ms = 200;
    std::atomic<int> calls{0};
    EXPECT_CALL(mds_service_, Heartbeat(_, _, _, _))
        .Times(6)
        .WillRepeatedly(Counting(
            &calls, ReplyAfter(std::chrono::milliseconds(400), pb::error::OK)));

    const int refreshes = mds_server_.mds_list_calls();
    Status status;
    const uint64_t elapsed_us = ElapsedUs([&]() {
      status = mds_client_->Heartbeat(kMemberId, kMemberIp, kMemberPort);
    });
    EXPECT_TRUE(status.IsInternal()) << status.ToString();
    EXPECT_EQ(calls, 6);
    EXPECT_EQ(mds_server_.mds_list_calls() - refreshes, 2);
    // 6 timeouts plus 3 backoffs of 100-200ms, never the 400ms reply
    EXPECT_GE(elapsed_us / 1000, (6 * 200) + (3 * 100));
    EXPECT_LT(elapsed_us / 1000, 6 * 400);
  }
}

TEST_F(MDSClientTest, Failover) {
  // two mds, both serving the same list
  MockMDSServer other;
  ASSERT_NO_FATAL_FAILURE(other.Start());
  const std::vector<pb::mds::MDS> mdses = {mds_server_.Entry(kMdsId),
                                           other.Entry(kMdsId + 1)};
  mds_server_.ServeMDSList(mdses);
  other.ServeMDSList(mdses);
  ASSERT_TRUE(mds_client_->Start().ok());

  std::atomic<int> calls{0};
  std::atomic<int> other_calls{0};
  {  // a failing mds is marked abnormal and skipped, without a refresh
    EXPECT_CALL(mds_service_, Heartbeat(_, _, _, _))
        .WillRepeatedly(
            Counting(&calls, Reply(pb::error::EINTERNAL, "store unavailable")));
    EXPECT_CALL(other.service(), Heartbeat(_, _, _, _))
        .WillRepeatedly(Counting(&other_calls, Reply(pb::error::OK)));
    const int refreshes = mds_server_.mds_list_calls() + other.mds_list_calls();

    // mds are picked at random: loop until the failing one is hit
    int rounds = 0;
    while (calls == 0 && rounds < 64) {
      EXPECT_TRUE(
          mds_client_->Heartbeat(kMemberId, kMemberIp, kMemberPort).ok())
          << rounds;
      rounds++;
    }
    ASSERT_EQ(calls, 2);

    for (int i = 0; i < 8; i++) {
      EXPECT_TRUE(
          mds_client_->Heartbeat(kMemberId, kMemberIp, kMemberPort).ok())
          << i;
    }
    EXPECT_EQ(calls, 2);
    EXPECT_EQ(other_calls, rounds + 8);
    EXPECT_EQ(mds_server_.mds_list_calls() + other.mds_list_calls(), refreshes);
  }

  {  // ENOT_SERVE refreshes the list and moves on to the first mds
    EXPECT_CALL(mds_service_, Heartbeat(_, _, _, _))
        .WillRepeatedly(Counting(&calls, Reply(pb::error::OK)));
    EXPECT_CALL(other.service(), Heartbeat(_, _, _, _))
        .WillOnce(
            Counting(&other_calls, Reply(pb::error::ENOT_SERVE, "not serving")))
        .WillRepeatedly(Counting(&other_calls, Reply(pb::error::OK)));
    calls = 0;
    other_calls = 0;
    const int refreshes = mds_server_.mds_list_calls() + other.mds_list_calls();

    EXPECT_TRUE(mds_client_->Heartbeat(kMemberId, kMemberIp, kMemberPort).ok());
    EXPECT_EQ(other_calls, 1);
    EXPECT_EQ(calls, 1);
    EXPECT_EQ(mds_server_.mds_list_calls() + other.mds_list_calls() - refreshes,
              1);
  }
}

TEST_F(MDSClientTest, ServerDown) {
  MockMDSServer other;
  ASSERT_NO_FATAL_FAILURE(other.Start());
  const std::vector<pb::mds::MDS> mdses = {mds_server_.Entry(kMdsId),
                                           other.Entry(kMdsId + 1)};
  mds_server_.ServeMDSList(mdses);
  other.ServeMDSList(mdses);
  ASSERT_TRUE(mds_client_->Start().ok());

  // a stopped mds is marked abnormal; the other takes every request
  other.Stop();
  std::atomic<int> calls{0};
  EXPECT_CALL(mds_service_, Heartbeat(_, _, _, _))
      .WillRepeatedly(Counting(&calls, Reply(pb::error::OK)));
  const int refreshes = mds_server_.mds_list_calls();
  for (int i = 0; i < 8; i++) {
    EXPECT_TRUE(mds_client_->Heartbeat(kMemberId, kMemberIp, kMemberPort).ok())
        << i;
  }
  EXPECT_EQ(calls, 8);
  EXPECT_EQ(mds_server_.mds_list_calls(), refreshes);
}

TEST_F(MDSClientTest, HeartbeatLatency) {
  ASSERT_TRUE(mds_client_->Start().ok());

  constexpr int kRounds = 200;
  EXPECT_CALL(mds_service_, Heartbeat(_, _, _, _))
      .Times(kRounds)
      .WillRepeatedly(Reply(pb::error::OK));

  const uint64_t elapsed_us = ElapsedUs([&]() {
    for (int i = 0; i < kRounds; i++) {
      ASSERT_TRUE(
          mds_client_->Heartbeat(kMemberId, kMemberIp, kMemberPort).ok())
          << i;
    }
  });
  std::printf(
      "[mds client, brpc over loopback]\n"
      "  heartbeat x%d : %8.2f ms  (%7.1f us each)\n",
      kRounds, elapsed_us / 1000.0, elapsed_us / static_cast<double>(kRounds));
}

}  // namespace blockcache
}  // namespace dingofs
