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

#include "metaserver/metaserver_service.h"

#include "metaserver/copyset/copyset_node_manager.h"
#include "metaserver/copyset/meta_operator.h"
#include "metaserver/metaservice_closure.h"

static bvar::LatencyRecorder g_oprequest_in_service_before_propose_latency(
    "oprequest_in_service_before_propose");

namespace dingofs {
namespace metaserver {

using copyset::BatchGetInodeAttrOperator;
using copyset::BatchGetXAttrOperator;
using copyset::CopysetNodeManager;
using copyset::CreateDentryOperator;
using copyset::CreateInodeOperator;
using copyset::CreateManageInodeOperator;
using copyset::CreatePartitionOperator;
using copyset::CreateRootInodeOperator;
using copyset::DeleteDentryOperator;
using copyset::DeleteDirQuotaOperator;
using copyset::DeleteInodeOperator;
using copyset::DeletePartitionOperator;
using copyset::FlushDirUsagesOperator;
using copyset::FlushFsUsageOperator;
using copyset::GetDentryOperator;
using copyset::GetDirQuotaOperator;
using copyset::GetFsQuotaOperator;
using copyset::GetInodeOperator;
using copyset::GetOrModifyS3ChunkInfoOperator;
using copyset::GetVolumeExtentOperator;
using copyset::ListDentryOperator;
using copyset::LoadDirQuotasOperator;
using copyset::PrepareRenameTxOperator;
using copyset::SetDirQuotaOperator;
using copyset::SetFsQuotaOperator;
using copyset::UpdateInodeOperator;
using copyset::UpdateVolumeExtentOperator;

namespace {

struct OperatorHelper {
  OperatorHelper(CopysetNodeManager* manager, InflightThrottle* throttle)
      : manager(manager), throttle(throttle) {}

  template <typename OperatorT, typename RequestT, typename ResponseT>
  void operator()(google::protobuf::RpcController* cntl,
                  const RequestT* request, ResponseT* response,
                  google::protobuf::Closure* done, PoolId poolId,
                  CopysetId copysetId) {
    butil::Timer timer;
    timer.start();
    // check if overloaded
    brpc::ClosureGuard doneGuard(done);
    if (throttle->IsOverLoad()) {
      LOG_EVERY_N(WARNING, 100)
          << "service overload, request: " << request->ShortDebugString();
      response->set_statuscode(pb::metaserver::MetaStatusCode::OVERLOAD);
      return;
    }

    auto* node = manager->GetCopysetNode(poolId, copysetId);

    if (!node) {
      LOG(WARNING) << "Copyset not found, request: "
                   << request->ShortDebugString();
      response->set_statuscode(
          pb::metaserver::MetaStatusCode::COPYSET_NOTEXIST);
      return;
    }

    auto* op =
        new OperatorT(node, cntl, request, response,
                      new MetaServiceClosure(throttle, doneGuard.release()));
    timer.stop();
    g_oprequest_in_service_before_propose_latency << timer.u_elapsed();
    node->GetMetric()->NewArrival(op->GetOperatorType());
    op->Propose();
  }

  CopysetNodeManager* manager;
  InflightThrottle* throttle;
};

}  // namespace

#define DEFINE_RPC_METHOD(method)                                            \
  void MetaServerServiceImpl::method(                                        \
      ::google::protobuf::RpcController* controller,                         \
      const pb::metaserver::method##Request* request,                        \
      pb::metaserver::method##Response* response,                            \
      ::google::protobuf::Closure* done) {                                   \
    OperatorHelper helper(copysetNodeManager_, inflightThrottle_);           \
    helper.operator()<method##Operator>(controller, request, response, done, \
                                        request->poolid(),                   \
                                        request->copysetid());               \
  }

DEFINE_RPC_METHOD(SetFsQuota);
DEFINE_RPC_METHOD(GetFsQuota);
DEFINE_RPC_METHOD(FlushFsUsage);
DEFINE_RPC_METHOD(SetDirQuota);
DEFINE_RPC_METHOD(GetDirQuota);
DEFINE_RPC_METHOD(DeleteDirQuota);
DEFINE_RPC_METHOD(LoadDirQuotas);
DEFINE_RPC_METHOD(FlushDirUsages);

void MetaServerServiceImpl::GetDentry(
    ::google::protobuf::RpcController* controller,
    const pb::metaserver::GetDentryRequest* request,
    pb::metaserver::GetDentryResponse* response,
    ::google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<GetDentryOperator>(controller, request, response, done,
                                       request->poolid(), request->copysetid());
}

void MetaServerServiceImpl::ListDentry(
    ::google::protobuf::RpcController* controller,
    const pb::metaserver::ListDentryRequest* request,
    pb::metaserver::ListDentryResponse* response,
    ::google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);

  helper.operator()<ListDentryOperator>(controller, request, response, done,
                                        request->poolid(),
                                        request->copysetid());
}

void MetaServerServiceImpl::CreateDentry(
    ::google::protobuf::RpcController* controller,
    const pb::metaserver::CreateDentryRequest* request,
    pb::metaserver::CreateDentryResponse* response,
    ::google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<CreateDentryOperator>(controller, request, response, done,
                                          request->poolid(),
                                          request->copysetid());
}

void MetaServerServiceImpl::DeleteDentry(
    ::google::protobuf::RpcController* controller,
    const pb::metaserver::DeleteDentryRequest* request,
    pb::metaserver::DeleteDentryResponse* response,
    ::google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<DeleteDentryOperator>(controller, request, response, done,
                                          request->poolid(),
                                          request->copysetid());
}

void MetaServerServiceImpl::GetInode(
    ::google::protobuf::RpcController* controller,
    const pb::metaserver::GetInodeRequest* request,
    pb::metaserver::GetInodeResponse* response,
    ::google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<GetInodeOperator>(controller, request, response, done,
                                      request->poolid(), request->copysetid());
}

void MetaServerServiceImpl::BatchGetInodeAttr(
    ::google::protobuf::RpcController* controller,
    const pb::metaserver::BatchGetInodeAttrRequest* request,
    pb::metaserver::BatchGetInodeAttrResponse* response,
    ::google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<BatchGetInodeAttrOperator>(controller, request, response,
                                               done, request->poolid(),
                                               request->copysetid());
}

void MetaServerServiceImpl::BatchGetXAttr(
    ::google::protobuf::RpcController* controller,
    const pb::metaserver::BatchGetXAttrRequest* request,
    pb::metaserver::BatchGetXAttrResponse* response,
    ::google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<BatchGetXAttrOperator>(controller, request, response, done,
                                           request->poolid(),
                                           request->copysetid());
}

void MetaServerServiceImpl::CreateInode(
    ::google::protobuf::RpcController* controller,
    const pb::metaserver::CreateInodeRequest* request,
    pb::metaserver::CreateInodeResponse* response,
    ::google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<CreateInodeOperator>(controller, request, response, done,
                                         request->poolid(),
                                         request->copysetid());
}

void MetaServerServiceImpl::CreateRootInode(
    ::google::protobuf::RpcController* controller,
    const pb::metaserver::CreateRootInodeRequest* request,
    pb::metaserver::CreateRootInodeResponse* response,
    ::google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<CreateRootInodeOperator>(controller, request, response,
                                             done, request->poolid(),
                                             request->copysetid());
}

void MetaServerServiceImpl::CreateManageInode(
    ::google::protobuf::RpcController* controller,
    const pb::metaserver::CreateManageInodeRequest* request,
    pb::metaserver::CreateManageInodeResponse* response,
    ::google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<CreateManageInodeOperator>(controller, request, response,
                                               done, request->poolid(),
                                               request->copysetid());
}

void MetaServerServiceImpl::UpdateInode(
    ::google::protobuf::RpcController* controller,
    const pb::metaserver::UpdateInodeRequest* request,
    pb::metaserver::UpdateInodeResponse* response,
    ::google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<UpdateInodeOperator>(controller, request, response, done,
                                         request->poolid(),
                                         request->copysetid());
}

void MetaServerServiceImpl::GetOrModifyS3ChunkInfo(
    ::google::protobuf::RpcController* controller,
    const pb::metaserver::GetOrModifyS3ChunkInfoRequest* request,
    pb::metaserver::GetOrModifyS3ChunkInfoResponse* response,
    ::google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<GetOrModifyS3ChunkInfoOperator>(
      controller, request, response, done, request->poolid(),
      request->copysetid());
}

void MetaServerServiceImpl::DeleteInode(
    ::google::protobuf::RpcController* controller,
    const pb::metaserver::DeleteInodeRequest* request,
    pb::metaserver::DeleteInodeResponse* response,
    ::google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<DeleteInodeOperator>(controller, request, response, done,
                                         request->poolid(),
                                         request->copysetid());
}

void MetaServerServiceImpl::CreatePartition(
    google::protobuf::RpcController* controller,
    const pb::metaserver::CreatePartitionRequest* request,
    pb::metaserver::CreatePartitionResponse* response,
    google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<CreatePartitionOperator>(
      controller, request, response, done, request->partition().poolid(),
      request->partition().copysetid());
}

void MetaServerServiceImpl::DeletePartition(
    google::protobuf::RpcController* controller,
    const pb::metaserver::DeletePartitionRequest* request,
    pb::metaserver::DeletePartitionResponse* response,
    google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<DeletePartitionOperator>(controller, request, response,
                                             done, request->poolid(),
                                             request->copysetid());
}

void MetaServerServiceImpl::PrepareRenameTx(
    google::protobuf::RpcController* controller,
    const pb::metaserver::PrepareRenameTxRequest* request,
    pb::metaserver::PrepareRenameTxResponse* response,
    google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<PrepareRenameTxOperator>(controller, request, response,
                                             done, request->poolid(),
                                             request->copysetid());
}

void MetaServerServiceImpl::GetVolumeExtent(
    ::google::protobuf::RpcController* controller,
    const pb::metaserver::GetVolumeExtentRequest* request,
    pb::metaserver::GetVolumeExtentResponse* response,
    ::google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<GetVolumeExtentOperator>(controller, request, response,
                                             done, request->poolid(),
                                             request->copysetid());
}

void MetaServerServiceImpl::UpdateVolumeExtent(
    ::google::protobuf::RpcController* controller,
    const pb::metaserver::UpdateVolumeExtentRequest* request,
    pb::metaserver::UpdateVolumeExtentResponse* response,
    ::google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<UpdateVolumeExtentOperator>(controller, request, response,
                                                done, request->poolid(),
                                                request->copysetid());
}

}  // namespace metaserver
}  // namespace dingofs
