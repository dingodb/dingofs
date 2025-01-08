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
 * @Date: 2021-06-24 16:55:17
 * @Author: chenwei
 */
#include "mds/fake_metaserver.h"

namespace dingofs {
namespace metaserver {

using pb::metaserver::MetaStatusCode;

void FakeMetaserverImpl::GetDentry(
    ::google::protobuf::RpcController* controller,
    const pb::metaserver::GetDentryRequest* request,
    pb::metaserver::GetDentryResponse* response,
    ::google::protobuf::Closure* done) {
  brpc::ClosureGuard doneGuard(done);
  // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
  MetaStatusCode status = MetaStatusCode::OK;
  response->set_statuscode(status);
  return;
}

void FakeMetaserverImpl::ListDentry(
    ::google::protobuf::RpcController* controller,
    const pb::metaserver::ListDentryRequest* request,
    pb::metaserver::ListDentryResponse* response,
    ::google::protobuf::Closure* done) {
  brpc::ClosureGuard doneGuard(done);
  // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
  MetaStatusCode status = MetaStatusCode::OK;
  response->set_statuscode(status);
  return;
}

void FakeMetaserverImpl::CreateDentry(
    ::google::protobuf::RpcController* controller,
    const pb::metaserver::CreateDentryRequest* request,
    pb::metaserver::CreateDentryResponse* response,
    ::google::protobuf::Closure* done) {
  brpc::ClosureGuard doneGuard(done);
  // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
  MetaStatusCode status = MetaStatusCode::OK;
  response->set_statuscode(status);
  return;
}

void FakeMetaserverImpl::DeleteDentry(
    ::google::protobuf::RpcController* controller,
    const pb::metaserver::DeleteDentryRequest* request,
    pb::metaserver::DeleteDentryResponse* response,
    ::google::protobuf::Closure* done) {
  brpc::ClosureGuard doneGuard(done);
  // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
  MetaStatusCode status = MetaStatusCode::OK;
  response->set_statuscode(status);
  return;
}

void FakeMetaserverImpl::GetInode(
    ::google::protobuf::RpcController* controller,
    const pb::metaserver::GetInodeRequest* request,
    pb::metaserver::GetInodeResponse* response,
    ::google::protobuf::Closure* done) {
  brpc::ClosureGuard doneGuard(done);
  // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
  MetaStatusCode status = MetaStatusCode::OK;
  response->set_statuscode(status);
  return;
}

void FakeMetaserverImpl::CreateInode(
    ::google::protobuf::RpcController* controller,
    const pb::metaserver::CreateInodeRequest* request,
    pb::metaserver::CreateInodeResponse* response,
    ::google::protobuf::Closure* done) {
  brpc::ClosureGuard doneGuard(done);
  // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
  MetaStatusCode status = MetaStatusCode::OK;
  response->set_statuscode(status);
  return;
}

void FakeMetaserverImpl::CreateRootInode(
    ::google::protobuf::RpcController* controller,
    const pb::metaserver::CreateRootInodeRequest* request,
    pb::metaserver::CreateRootInodeResponse* response,
    ::google::protobuf::Closure* done) {
  brpc::ClosureGuard doneGuard(done);
  // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
  MetaStatusCode status = MetaStatusCode::OK;
  response->set_statuscode(status);
  return;
}

void FakeMetaserverImpl::UpdateInode(
    ::google::protobuf::RpcController* controller,
    const pb::metaserver::UpdateInodeRequest* request,
    pb::metaserver::UpdateInodeResponse* response,
    ::google::protobuf::Closure* done) {
  brpc::ClosureGuard doneGuard(done);
  // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
  MetaStatusCode status = MetaStatusCode::OK;
  response->set_statuscode(status);
  return;
}

void FakeMetaserverImpl::DeleteInode(
    ::google::protobuf::RpcController* controller,
    const pb::metaserver::DeleteInodeRequest* request,
    pb::metaserver::DeleteInodeResponse* response,
    ::google::protobuf::Closure* done) {
  brpc::ClosureGuard doneGuard(done);
  // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
  MetaStatusCode status = MetaStatusCode::OK;
  response->set_statuscode(status);
  return;
}
}  // namespace metaserver
}  // namespace dingofs
