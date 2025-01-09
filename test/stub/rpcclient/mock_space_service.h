/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Date: Wednesday Mar 23 19:20:53 CST 2022
 * Author: wuhanqing
 */

#ifndef DINGOFS_TEST_CLIENT_RPCCLIENT_MOCK_SPACE_SERVICE_H_
#define DINGOFS_TEST_CLIENT_RPCCLIENT_MOCK_SPACE_SERVICE_H_

#include <gmock/gmock.h>

#include "proto/space.pb.h"

namespace dingofs {
namespace mds {
namespace space {

class MockSpaceService : public pb::mds::space::SpaceService {
 public:
  MOCK_METHOD4(AllocateBlockGroup,
               void(google::protobuf::RpcController* controller,
                    const pb::mds::space::AllocateBlockGroupRequest* request,
                    pb::mds::space::AllocateBlockGroupResponse* response,
                    google::protobuf::Closure* done));

  MOCK_METHOD4(AcquireBlockGroup,
               void(google::protobuf::RpcController* controller,
                    const pb::mds::space::AcquireBlockGroupRequest* request,
                    pb::mds::space::AcquireBlockGroupResponse* response,
                    google::protobuf::Closure* done));

  MOCK_METHOD4(ReleaseBlockGroup,
               void(google::protobuf::RpcController* controller,
                    const pb::mds::space::ReleaseBlockGroupRequest* request,
                    pb::mds::space::ReleaseBlockGroupResponse* response,
                    google::protobuf::Closure* done));

  MOCK_METHOD4(StatSpace, void(google::protobuf::RpcController* controller,
                               const pb::mds::space::StatSpaceRequest* request,
                               pb::mds::space::StatSpaceResponse* response,
                               google::protobuf::Closure* done));

  MOCK_METHOD4(UpdateUsage,
               void(google::protobuf::RpcController* controller,
                    const pb::mds::space::UpdateUsageRequest* request,
                    pb::mds::space::UpdateUsageResponse* response,
                    google::protobuf::Closure* done));
};

}  // namespace space
}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_TEST_CLIENT_RPCCLIENT_MOCK_SPACE_SERVICE_H_
