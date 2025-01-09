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
 * Created Date: Mon Sept 4 2021
 * Author: lixiaocui
 */

#ifndef DINGOFS_TEST_CLIENT_RPCCLIENT_MOCK_CLI2_SERVICE_H_
#define DINGOFS_TEST_CLIENT_RPCCLIENT_MOCK_CLI2_SERVICE_H_

#include <gmock/gmock.h>

#include "dingofs/cli2.pb.h"

namespace dingofs {
namespace stub {
namespace rpcclient {
class MockCliService2 : public pb::metaserver::copyset::CliService2 {
 public:
  MockCliService2() = default;
  ~MockCliService2() override = default;

  MOCK_METHOD4(GetLeader,
               void(::google::protobuf::RpcController* controller,
                    const pb::metaserver::copyset::GetLeaderRequest2* request,
                    pb::metaserver::copyset::GetLeaderResponse2* response,
                    ::google::protobuf::Closure* done));
};
}  // namespace rpcclient
}  // namespace stub
}  // namespace dingofs

#endif  // DINGOFS_TEST_CLIENT_RPCCLIENT_MOCK_CLI2_SERVICE_H_
