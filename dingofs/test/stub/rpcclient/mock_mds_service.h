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
 * Project: dingo
 * Created Date: Thur Jun 16 2021
 * Author: lixiaocui
 */

#ifndef DINGOFS_TEST_CLIENT_RPCCLIENT_MOCK_MDS_SERVICE_H_
#define DINGOFS_TEST_CLIENT_RPCCLIENT_MOCK_MDS_SERVICE_H_

#include <gmock/gmock.h>

#include "proto/mds.pb.h"

namespace dingofs {
namespace stub {
namespace rpcclient {
class MockMdsService : public pb::mds::MdsService {
 public:
  MockMdsService() = default;
  ~MockMdsService() override = default;

  MOCK_METHOD4(MountFs, void(::google::protobuf::RpcController* controller,
                             const pb::mds::MountFsRequest* request,
                             pb::mds::MountFsResponse* response,
                             ::google::protobuf::Closure* done));
  MOCK_METHOD4(UmountFs, void(::google::protobuf::RpcController* controller,
                              const pb::mds::UmountFsRequest* request,
                              pb::mds::UmountFsResponse* response,
                              ::google::protobuf::Closure* done));
  MOCK_METHOD4(GetFsInfo, void(::google::protobuf::RpcController* controller,
                               const pb::mds::GetFsInfoRequest* request,
                               pb::mds::GetFsInfoResponse* response,
                               ::google::protobuf::Closure* done));

  MOCK_METHOD4(RefreshSession,
               void(::google::protobuf::RpcController* controller,
                    const pb::mds::RefreshSessionRequest* request,
                    pb::mds::RefreshSessionResponse* response,
                    ::google::protobuf::Closure* done));
};
}  // namespace rpcclient
}  // namespace stub
}  // namespace dingofs

#endif  // DINGOFS_TEST_CLIENT_RPCCLIENT_MOCK_MDS_SERVICE_H_
