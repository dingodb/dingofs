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
 * Created Date: 2021-09-05
 * Author: wanghai01
 */

#ifndef DINGOFS_TEST_TOOLS_MOCK_TOPOLOGY_SERVICE_H_
#define DINGOFS_TEST_TOOLS_MOCK_TOPOLOGY_SERVICE_H_

#include <gmock/gmock.h>

#include "dingofs/topology.pb.h"

namespace dingofs {
namespace mds {
namespace topology {

using ::google::protobuf::Closure;
using ::google::protobuf::RpcController;

class MockTopologyService : public pb::mds::topology::TopologyService {
 public:
  MockTopologyService() = default;
  ~MockTopologyService() override = default;

  MOCK_METHOD4(ListPool, void(RpcController* controller,
                              const pb::mds::topology::ListPoolRequest* request,
                              pb::mds::topology::ListPoolResponse* response,
                              Closure* done));
  MOCK_METHOD4(CreatePool,
               void(RpcController* controller,
                    const pb::mds::topology::CreatePoolRequest* request,
                    pb::mds::topology::CreatePoolResponse* response,
                    Closure* done));
  MOCK_METHOD4(ListPoolZone,
               void(RpcController* controller,
                    const pb::mds::topology::ListPoolZoneRequest* request,
                    pb::mds::topology::ListPoolZoneResponse* response,
                    Closure* done));
  MOCK_METHOD4(CreateZone,
               void(RpcController* controller,
                    const pb::mds::topology::CreateZoneRequest* request,
                    pb::mds::topology::CreateZoneResponse* response,
                    Closure* done));
  MOCK_METHOD4(ListZoneServer,
               void(RpcController* controller,
                    const pb::mds::topology::ListZoneServerRequest* request,
                    pb::mds::topology::ListZoneServerResponse* response,
                    Closure* done));
  MOCK_METHOD4(RegistServer,
               void(RpcController* controller,
                    const pb::mds::topology::ServerRegistRequest* request,
                    pb::mds::topology::ServerRegistResponse* response,
                    Closure* done));
  MOCK_METHOD4(ListMetaServer,
               void(RpcController* controller,
                    const pb::mds::topology::ListMetaServerRequest* request,
                    pb::mds::topology::ListMetaServerResponse* response,
                    Closure* done));
  MOCK_METHOD4(RegistMetaServer,
               void(RpcController* controller,
                    const pb::mds::topology::MetaServerRegistRequest* request,
                    pb::mds::topology::MetaServerRegistResponse* response,
                    Closure* done));
};

}  // namespace topology
}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_TEST_TOOLS_MOCK_TOPOLOGY_SERVICE_H_
