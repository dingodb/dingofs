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
 * @Date: 2021-09-07
 * @Author: wanghai01
 */

#ifndef DINGOFS_TEST_MDS_MOCK_MOCK_CLI2_H_
#define DINGOFS_TEST_MDS_MOCK_MOCK_CLI2_H_
#include <gmock/gmock.h>

#include "dingofs/cli2.pb.h"

namespace dingofs {
namespace metaserver {
namespace copyset {

class MockCliService2 : public pb::metaserver::copyset::CliService2 {
 public:
  MOCK_METHOD4(GetLeader,
               void(::google::protobuf::RpcController* controller,
                    const pb::metaserver::copyset::GetLeaderRequest2* request,
                    pb::metaserver::copyset::GetLeaderResponse2* response,
                    ::google::protobuf::Closure* done));
};

}  // namespace copyset
}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_TEST_MDS_MOCK_MOCK_CLI2_H_
