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

#ifndef DINGOFS_TEST_CLIENT_RPCCLIENT_MOCK_METASERVER_SERVICE_H_
#define DINGOFS_TEST_CLIENT_RPCCLIENT_MOCK_METASERVER_SERVICE_H_

#include <gmock/gmock.h>

#include "dingofs/metaserver.pb.h"

namespace dingofs {
namespace stub {
namespace rpcclient {
class MockMetaServerService : public pb::metaserver::MetaServerService {
 public:
  MockMetaServerService() = default;
  ~MockMetaServerService() override = default;

  MOCK_METHOD4(GetDentry, void(::google::protobuf::RpcController* controller,
                               const pb::metaserver::GetDentryRequest* request,
                               pb::metaserver::GetDentryResponse* response,
                               ::google::protobuf::Closure* done));
  MOCK_METHOD4(ListDentry,
               void(::google::protobuf::RpcController* controller,
                    const pb::metaserver::ListDentryRequest* request,
                    pb::metaserver::ListDentryResponse* response,
                    ::google::protobuf::Closure* done));
  MOCK_METHOD4(CreateDentry,
               void(::google::protobuf::RpcController* controller,
                    const pb::metaserver::CreateDentryRequest* request,
                    pb::metaserver::CreateDentryResponse* response,
                    ::google::protobuf::Closure* done));
  MOCK_METHOD4(DeleteDentry,
               void(::google::protobuf::RpcController* controller,
                    const pb::metaserver::DeleteDentryRequest* request,
                    pb::metaserver::DeleteDentryResponse* response,
                    ::google::protobuf::Closure* done));

  MOCK_METHOD4(PrepareRenameTx,
               void(::google::protobuf::RpcController* controller,
                    const pb::metaserver::PrepareRenameTxRequest* request,
                    pb::metaserver::PrepareRenameTxResponse* response,
                    ::google::protobuf::Closure* done));

  MOCK_METHOD4(GetInode, void(::google::protobuf::RpcController* controller,
                              const pb::metaserver::GetInodeRequest* request,
                              pb::metaserver::GetInodeResponse* response,
                              ::google::protobuf::Closure* done));

  MOCK_METHOD4(BatchGetInodeAttr,
               void(::google::protobuf::RpcController* controller,
                    const pb::metaserver::BatchGetInodeAttrRequest* request,
                    pb::metaserver::BatchGetInodeAttrResponse* response,
                    ::google::protobuf::Closure* done));

  MOCK_METHOD4(BatchGetXAttr,
               void(::google::protobuf::RpcController* controller,
                    const pb::metaserver::BatchGetXAttrRequest* request,
                    pb::metaserver::BatchGetXAttrResponse* response,
                    ::google::protobuf::Closure* done));

  MOCK_METHOD4(CreateInode,
               void(::google::protobuf::RpcController* controller,
                    const pb::metaserver::CreateInodeRequest* request,
                    pb::metaserver::CreateInodeResponse* response,
                    ::google::protobuf::Closure* done));
  MOCK_METHOD4(UpdateInode,
               void(::google::protobuf::RpcController* controller,
                    const pb::metaserver::UpdateInodeRequest* request,
                    pb::metaserver::UpdateInodeResponse* response,
                    ::google::protobuf::Closure* done));
  MOCK_METHOD4(DeleteInode,
               void(::google::protobuf::RpcController* controller,
                    const pb::metaserver::DeleteInodeRequest* request,
                    pb::metaserver::DeleteInodeResponse* response,
                    ::google::protobuf::Closure* done));

  MOCK_METHOD4(
      GetOrModifyS3ChunkInfo,
      void(::google::protobuf::RpcController* controller,
           const pb::metaserver::GetOrModifyS3ChunkInfoRequest* request,
           pb::metaserver::GetOrModifyS3ChunkInfoResponse* response,
           ::google::protobuf::Closure* done));

  MOCK_METHOD4(GetVolumeExtent,
               void(::google::protobuf::RpcController* controller,
                    const pb::metaserver::GetVolumeExtentRequest* request,
                    pb::metaserver::GetVolumeExtentResponse* response,
                    ::google::protobuf::Closure* done));

  MOCK_METHOD4(UpdateVolumeExtent,
               void(::google::protobuf::RpcController* controller,
                    const pb::metaserver::UpdateVolumeExtentRequest* request,
                    pb::metaserver::UpdateVolumeExtentResponse* response,
                    ::google::protobuf::Closure* done));
};
}  // namespace rpcclient
}  // namespace stub
}  // namespace dingofs

#endif  // DINGOFS_TEST_CLIENT_RPCCLIENT_MOCK_METASERVER_SERVICE_H_
