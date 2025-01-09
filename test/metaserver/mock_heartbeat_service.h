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
 * @Date: 2021-09-29 10:15:43
 * @Author: chenwei
 */

#ifndef DINGOFS_TEST_METASERVER_MOCK_HEARTBEAT_SERVICE_H_
#define DINGOFS_TEST_METASERVER_MOCK_HEARTBEAT_SERVICE_H_

#include <gmock/gmock.h>

#include "proto/heartbeat.pb.h"

namespace dingofs {
namespace mds {
namespace heartbeat {

class MockHeartbeatService : public pb::mds::heartbeat::HeartbeatService {
 public:
  MockHeartbeatService() = default;
  ~MockHeartbeatService() override = default;

  MOCK_METHOD4(
      MetaServerHeartbeat,
      void(google::protobuf::RpcController* cntl_base,
           const pb::mds::heartbeat::MetaServerHeartbeatRequest* request,
           pb::mds::heartbeat::MetaServerHeartbeatResponse* response,
           google::protobuf::Closure* done));
};
}  // namespace heartbeat
}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_TEST_METASERVER_MOCK_HEARTBEAT_SERVICE_H_
