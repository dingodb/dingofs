/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Project: curve
 * File Created: 2019-12-23
 * Author: charisu
 */

#ifndef TEST_TOOLS_MOCK_MOCK_CLI_SERVICE_H_
#define TEST_TOOLS_MOCK_MOCK_CLI_SERVICE_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "proto/cli2.pb.h"

namespace curve {
namespace tool {

using ::curve::chunkserver::CliService2;
using ::curve::chunkserver::GetLeaderRequest2;
using ::curve::chunkserver::GetLeaderResponse2;
using ::curve::chunkserver::RemovePeerRequest2;
using ::curve::chunkserver::RemovePeerResponse2;
using ::curve::chunkserver::ResetPeerRequest2;
using ::curve::chunkserver::ResetPeerResponse2;
using ::curve::chunkserver::SnapshotAllRequest;
using ::curve::chunkserver::SnapshotAllResponse;
using ::curve::chunkserver::SnapshotRequest2;
using ::curve::chunkserver::SnapshotResponse2;
using ::curve::chunkserver::TransferLeaderRequest2;
using ::curve::chunkserver::TransferLeaderResponse2;
using ::google::protobuf::Closure;
using ::google::protobuf::RpcController;

class MockCliService : public CliService2 {
 public:
  MOCK_METHOD4(GetLeader,
               void(RpcController* controller, const GetLeaderRequest2* request,
                    GetLeaderResponse2* response, Closure* done));

  MOCK_METHOD4(RemovePeer, void(RpcController* controller,
                                const RemovePeerRequest2* request,
                                RemovePeerResponse2* response, Closure* done));

  MOCK_METHOD4(TransferLeader,
               void(RpcController* controller,
                    const TransferLeaderRequest2* request,
                    TransferLeaderResponse2* response, Closure* done));

  MOCK_METHOD4(ResetPeer,
               void(RpcController* controller, const ResetPeerRequest2* request,
                    ResetPeerResponse2* response, Closure* done));

  MOCK_METHOD4(Snapshot,
               void(RpcController* controller, const SnapshotRequest2* request,
                    SnapshotResponse2* response, Closure* done));

  MOCK_METHOD4(SnapshotAll, void(RpcController* controller,
                                 const SnapshotAllRequest* request,
                                 SnapshotAllResponse* response, Closure* done));
};
}  // namespace tool
}  // namespace curve
#endif  // TEST_TOOLS_MOCK_MOCK_CLI_SERVICE_H_
