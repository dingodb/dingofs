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
 * @Date: 2021-06-24 16:26:02
 * @Author: chenwei
 */

#ifndef DINGOFS_TEST_MDS_MOCK_MOCK_METASERVER_H_
#define DINGOFS_TEST_MDS_MOCK_MOCK_METASERVER_H_
#include <gmock/gmock.h>

#include "dingofs/copyset.pb.h"
#include "dingofs/metaserver.pb.h"

namespace dingofs {
namespace metaserver {
class MockMetaserverService : public pb::metaserver::MetaServerService {
 public:
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
  MOCK_METHOD4(GetInode, void(::google::protobuf::RpcController* controller,
                              const pb::metaserver::GetInodeRequest* request,
                              pb::metaserver::GetInodeResponse* response,
                              ::google::protobuf::Closure* done));
  MOCK_METHOD4(CreateInode,
               void(::google::protobuf::RpcController* controller,
                    const pb::metaserver::CreateInodeRequest* request,
                    pb::metaserver::CreateInodeResponse* response,
                    ::google::protobuf::Closure* done));
  MOCK_METHOD4(CreateRootInode,
               void(::google::protobuf::RpcController* controller,
                    const pb::metaserver::CreateRootInodeRequest* request,
                    pb::metaserver::CreateRootInodeResponse* response,
                    ::google::protobuf::Closure* done));
  MOCK_METHOD4(CreateManageInode,
               void(::google::protobuf::RpcController* controller,
                    const pb::metaserver::CreateManageInodeRequest* request,
                    pb::metaserver::CreateManageInodeResponse* response,
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
  MOCK_METHOD4(CreatePartition,
               void(::google::protobuf::RpcController* controller,
                    const pb::metaserver::CreatePartitionRequest* request,
                    pb::metaserver::CreatePartitionResponse* response,
                    ::google::protobuf::Closure* done));
  MOCK_METHOD4(DeletePartition,
               void(::google::protobuf::RpcController* controller,
                    const pb::metaserver::DeletePartitionRequest* request,
                    pb::metaserver::DeletePartitionResponse* response,
                    ::google::protobuf::Closure* done));
};

namespace copyset {
class MockCopysetService : public pb::metaserver::copyset::CopysetService {
 public:
  MOCK_METHOD4(
      CreateCopysetNode,
      void(::google::protobuf::RpcController* controller,
           const pb::metaserver::copyset::CreateCopysetRequest* request,
           pb::metaserver::copyset::CreateCopysetResponse* response,
           ::google::protobuf::Closure* done));
};

}  // namespace copyset
}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_TEST_MDS_MOCK_MOCK_METASERVER_H_
