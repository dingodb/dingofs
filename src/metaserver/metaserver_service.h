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
 * Created Date: 2021-05-19
 * Author: chenwei
 */

#ifndef DINGOFS_SRC_METASERVER_METASERVER_SERVICE_H_
#define DINGOFS_SRC_METASERVER_METASERVER_SERVICE_H_

#include <brpc/closure_guard.h>
#include <brpc/controller.h>

#include "proto/metaserver.pb.h"
#include "metaserver/copyset/copyset_node_manager.h"
#include "metaserver/inflight_throttle.h"

namespace dingofs {
namespace metaserver {

#define DECLARE_RPC_METHOD(method)                            \
  void method(::google::protobuf::RpcController* controller,  \
              const pb::metaserver::method##Request* request, \
              pb::metaserver::method##Response* response,     \
              ::google::protobuf::Closure* done) override

class MetaServerServiceImpl : public pb::metaserver::MetaServerService {
 public:
  MetaServerServiceImpl(copyset::CopysetNodeManager* copysetNodeManager,
                        InflightThrottle* inflightThrottle)
      : copysetNodeManager_(copysetNodeManager),
        inflightThrottle_(inflightThrottle) {}

  DECLARE_RPC_METHOD(SetFsQuota);
  DECLARE_RPC_METHOD(GetFsQuota);
  DECLARE_RPC_METHOD(FlushFsUsage);
  DECLARE_RPC_METHOD(SetDirQuota);
  DECLARE_RPC_METHOD(GetDirQuota);
  DECLARE_RPC_METHOD(DeleteDirQuota);
  DECLARE_RPC_METHOD(LoadDirQuotas);
  DECLARE_RPC_METHOD(FlushDirUsages);

  void GetDentry(::google::protobuf::RpcController* controller,
                 const pb::metaserver::GetDentryRequest* request,
                 pb::metaserver::GetDentryResponse* response,
                 ::google::protobuf::Closure* done) override;
  void ListDentry(::google::protobuf::RpcController* controller,
                  const pb::metaserver::ListDentryRequest* request,
                  pb::metaserver::ListDentryResponse* response,
                  ::google::protobuf::Closure* done) override;
  void CreateDentry(::google::protobuf::RpcController* controller,
                    const pb::metaserver::CreateDentryRequest* request,
                    pb::metaserver::CreateDentryResponse* response,
                    ::google::protobuf::Closure* done) override;
  void DeleteDentry(::google::protobuf::RpcController* controller,
                    const pb::metaserver::DeleteDentryRequest* request,
                    pb::metaserver::DeleteDentryResponse* response,
                    ::google::protobuf::Closure* done) override;
  void GetInode(::google::protobuf::RpcController* controller,
                const pb::metaserver::GetInodeRequest* request,
                pb::metaserver::GetInodeResponse* response,
                ::google::protobuf::Closure* done) override;
  void BatchGetInodeAttr(
      ::google::protobuf::RpcController* controller,
      const pb::metaserver::BatchGetInodeAttrRequest* request,
      pb::metaserver::BatchGetInodeAttrResponse* response,
      ::google::protobuf::Closure* done) override;
  void BatchGetXAttr(::google::protobuf::RpcController* controller,
                     const pb::metaserver::BatchGetXAttrRequest* request,
                     pb::metaserver::BatchGetXAttrResponse* response,
                     ::google::protobuf::Closure* done) override;
  void CreateInode(::google::protobuf::RpcController* controller,
                   const pb::metaserver::CreateInodeRequest* request,
                   pb::metaserver::CreateInodeResponse* response,
                   ::google::protobuf::Closure* done) override;
  void CreateRootInode(::google::protobuf::RpcController* controller,
                       const pb::metaserver::CreateRootInodeRequest* request,
                       pb::metaserver::CreateRootInodeResponse* response,
                       ::google::protobuf::Closure* done) override;
  void CreateManageInode(
      ::google::protobuf::RpcController* controller,
      const pb::metaserver::CreateManageInodeRequest* request,
      pb::metaserver::CreateManageInodeResponse* response,
      ::google::protobuf::Closure* done) override;
  void UpdateInode(::google::protobuf::RpcController* controller,
                   const pb::metaserver::UpdateInodeRequest* request,
                   pb::metaserver::UpdateInodeResponse* response,
                   ::google::protobuf::Closure* done) override;
  void GetOrModifyS3ChunkInfo(
      ::google::protobuf::RpcController* controller,
      const pb::metaserver::GetOrModifyS3ChunkInfoRequest* request,
      pb::metaserver::GetOrModifyS3ChunkInfoResponse* response,
      ::google::protobuf::Closure* done) override;
  void DeleteInode(::google::protobuf::RpcController* controller,
                   const pb::metaserver::DeleteInodeRequest* request,
                   pb::metaserver::DeleteInodeResponse* response,
                   ::google::protobuf::Closure* done) override;

  void CreatePartition(google::protobuf::RpcController* controller,
                       const pb::metaserver::CreatePartitionRequest* request,
                       pb::metaserver::CreatePartitionResponse* response,
                       google::protobuf::Closure* done) override;

  void DeletePartition(google::protobuf::RpcController* controller,
                       const pb::metaserver::DeletePartitionRequest* request,
                       pb::metaserver::DeletePartitionResponse* response,
                       google::protobuf::Closure* done) override;

  void PrepareRenameTx(google::protobuf::RpcController* controller,
                       const pb::metaserver::PrepareRenameTxRequest* request,
                       pb::metaserver::PrepareRenameTxResponse* response,
                       google::protobuf::Closure* done) override;

  void GetVolumeExtent(::google::protobuf::RpcController* controller,
                       const pb::metaserver::GetVolumeExtentRequest* request,
                       pb::metaserver::GetVolumeExtentResponse* response,
                       ::google::protobuf::Closure* done) override;

  void UpdateVolumeExtent(
      ::google::protobuf::RpcController* controller,
      const pb::metaserver::UpdateVolumeExtentRequest* request,
      pb::metaserver::UpdateVolumeExtentResponse* response,
      ::google::protobuf::Closure* done) override;

 private:
  copyset::CopysetNodeManager* copysetNodeManager_;
  InflightThrottle* inflightThrottle_;
};
}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_SRC_METASERVER_METASERVER_SERVICE_H_
