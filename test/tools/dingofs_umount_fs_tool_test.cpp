/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * @Project: dingo
 * @Date: 2021-09-27
 * @Author: chengyi01
 */

#include "tools/umount/dingofs_umount_fs_tool.h"

#include <brpc/controller.h>
#include <brpc/server.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <iostream>
#include <queue>

#include "dingofs/mds.pb.h"
#include "mds/common/mds_define.h"
#include "tools/mock_mds_service.h"

DECLARE_string(mdsAddr);
DECLARE_string(mountpoint);
DECLARE_string(fsName);

namespace dingofs {
namespace tools {
namespace umount {

using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::SetArgPointee;

using pb::mds::FSStatusCode;
using pb::mds::Mountpoint;

class UmountfsToolTest : public testing::Test {
 protected:
  void SetUp() override {
    ASSERT_EQ(0, server_.AddService(&mockMdsService_,
                                    brpc::SERVER_DOESNT_OWN_SERVICE));
    uint16_t port = 56800;
    int ret = 0;
    while (port < 65535) {
      addr_ = "127.0.0.1:" + std::to_string(port);
      ret = server_.Start(addr_.c_str(), nullptr);
      if (ret >= 0) {
        LOG(INFO) << "service success, listen port = " << port;
        break;
      }
      ++port;
    }

    ASSERT_EQ(0, ret);
  }
  void TearDown() override {
    server_.Stop(0);
    server_.Join();
  }

 protected:
  std::string addr_;
  brpc::Server server_;
  MockMdsService mockMdsService_;
  UmountFsTool ut_;
};

void UF(::google::protobuf::RpcController* controller,
        const pb::mds::UmountFsRequest* request,
        pb::mds::UmountFsResponse* response,
        ::google::protobuf::Closure* done) {
  done->Run();
}

TEST_F(UmountfsToolTest, test_umount_success) {
  FLAGS_mdsAddr = addr_;
  FLAGS_fsName = "test";
  pb::mds::UmountFsResponse response;
  response.set_statuscode(pb::mds::FSStatusCode::OK);
  EXPECT_CALL(mockMdsService_, UmountFs(_, _, _, _))
      .WillRepeatedly(DoAll(SetArgPointee<2>(response), Invoke(UF)));
  int ret = ut_.Run();
  ASSERT_EQ(ret, 0);
}

// mountpoint not exist
TEST_F(UmountfsToolTest, test_umount_failed) {
  FLAGS_mdsAddr = addr_;
  FLAGS_fsName = "test";
  pb::mds::UmountFsResponse response;
  response.set_statuscode(FSStatusCode::MOUNT_POINT_NOT_EXIST);
  EXPECT_CALL(mockMdsService_, UmountFs(_, _, _, _))
      .WillRepeatedly(DoAll(SetArgPointee<2>(response), Invoke(UF)));
  int ret = ut_.Run();
  ASSERT_EQ(ret, -1);
}

// connect to mds failed
TEST_F(UmountfsToolTest, test_umount_connect_failed) {
  FLAGS_mdsAddr = "127.0.0.1:6700";
  FLAGS_fsName = "test";
  int ret = ut_.Run();
  ASSERT_EQ(ret, -1);
}

// init failed
TEST_F(UmountfsToolTest, test_umount_init_name) {
  FLAGS_mdsAddr = "abcd";
  FLAGS_fsName = "test";
  int ret = ut_.Run();
  ASSERT_EQ(ret, -1);
}

// invalid mountpoint
TEST_F(UmountfsToolTest, test_umount_invalid_mountpoint) {
  FLAGS_mdsAddr = addr_;
  FLAGS_mountpoint = "/1234/";
  FLAGS_fsName = "test";
  int ret = ut_.Run();
  ASSERT_EQ(ret, -1);
  FLAGS_mountpoint = "127.0.0.1:/mnt/dingofs-umount-test";
}

// init
TEST_F(UmountfsToolTest, test_umount_tool_init) {
  FLAGS_mdsAddr = addr_;
  FLAGS_fsName = "test";

  std::shared_ptr<brpc::Channel> channel = std::make_shared<brpc::Channel>();
  std::shared_ptr<brpc::Controller> controller =
      std::make_shared<brpc::Controller>();
  pb::mds::UmountFsRequest request;
  request.set_fsname("123");
  auto* mp = new Mountpoint();
  mp->set_hostname("0.0.0.0");
  mp->set_port(9000);
  mp->set_path("/data");
  request.set_allocated_mountpoint(mp);
  std::queue<pb::mds::UmountFsRequest> requestQueue;
  requestQueue.push(request);
  std::shared_ptr<pb::mds::UmountFsResponse> response =
      std::make_shared<pb::mds::UmountFsResponse>();

  pb::mds::UmountFsResponse re;
  re.set_statuscode(FSStatusCode::OK);
  EXPECT_CALL(mockMdsService_, UmountFs(_, _, _, _))
      .WillRepeatedly(DoAll(SetArgPointee<2>(re), Invoke(UF)));

  std::shared_ptr<pb::mds::MdsService_Stub> service_stub =
      std::make_shared<pb::mds::MdsService_Stub>(channel.get());

  ut_.CurvefsToolRpc::Init(
      channel, controller, requestQueue, response, service_stub,
      std::bind(&pb::mds::MdsService_Stub::UmountFs, service_stub.get(),
                std::placeholders::_1, std::placeholders::_2,
                std::placeholders::_3, nullptr),
      nullptr);

  int ret = ut_.RunCommand();
  ASSERT_EQ(ret, 0);
}

}  // namespace umount
}  // namespace tools
}  // namespace dingofs

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
