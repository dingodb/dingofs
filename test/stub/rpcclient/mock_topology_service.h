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
 * @Date: 2021-09-14 16:54:35
 * @Author: chenwei
 */

#ifndef DINGOFS_TEST_CLIENT_RPCCLIENT_MOCK_TOPOLOGY_SERVICE_H_
#define DINGOFS_TEST_CLIENT_RPCCLIENT_MOCK_TOPOLOGY_SERVICE_H_

#include <gmock/gmock.h>

#include "dingofs/topology.pb.h"

namespace dingofs {
namespace stub {
namespace rpcclient {
class MockTopologyService : public pb::mds::topology::TopologyService {
 public:
  MockTopologyService() = default;
  ~MockTopologyService() override = default;

  MOCK_METHOD4(GetMetaServer,
               void(::google::protobuf::RpcController* controller,
                    const pb::mds::topology::GetMetaServerInfoRequest* request,
                    pb::mds::topology::GetMetaServerInfoResponse* response,
                    ::google::protobuf::Closure* done));
  MOCK_METHOD4(
      GetMetaServerListInCopysets,
      void(::google::protobuf::RpcController* controller,
           const pb::mds::topology::GetMetaServerListInCopySetsRequest* request,
           pb::mds::topology::GetMetaServerListInCopySetsResponse* response,
           ::google::protobuf::Closure* done));
  MOCK_METHOD4(CreatePartition,
               void(::google::protobuf::RpcController* controller,
                    const pb::mds::topology::CreatePartitionRequest* request,
                    pb::mds::topology::CreatePartitionResponse* response,
                    ::google::protobuf::Closure* done));
  MOCK_METHOD4(ListPartition,
               void(::google::protobuf::RpcController* controller,
                    const pb::mds::topology::ListPartitionRequest* request,
                    pb::mds::topology::ListPartitionResponse* response,
                    ::google::protobuf::Closure* done));
  MOCK_METHOD4(
      GetCopysetOfPartition,
      void(::google::protobuf::RpcController* controller,
           const pb::mds::topology::GetCopysetOfPartitionRequest* request,
           pb::mds::topology::GetCopysetOfPartitionResponse* response,
           ::google::protobuf::Closure* done));
  MOCK_METHOD4(
      AllocOrGetMemcacheCluster,
      void(::google::protobuf::RpcController* controller,
           const pb::mds::topology::AllocOrGetMemcacheClusterRequest* request,
           pb::mds::topology::AllocOrGetMemcacheClusterResponse* response,
           ::google::protobuf::Closure* done));
};
}  // namespace rpcclient
}  // namespace stub
}  // namespace dingofs

#endif  // DINGOFS_TEST_CLIENT_RPCCLIENT_MOCK_TOPOLOGY_SERVICE_H_
