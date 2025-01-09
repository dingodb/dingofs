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
 * @Date: 2021-06-24 16:53:31
 * @Author: chenwei
 */
#ifndef DINGOFS_TEST_MDS_FAKE_METASERVER_H_
#define DINGOFS_TEST_MDS_FAKE_METASERVER_H_

#include <brpc/closure_guard.h>
#include <brpc/controller.h>

#include "dingofs/metaserver.pb.h"

namespace dingofs {
namespace metaserver {
class FakeMetaserverImpl : public pb::metaserver::MetaServerService {
 public:
  FakeMetaserverImpl() = default;

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
  void CreateInode(::google::protobuf::RpcController* controller,
                   const pb::metaserver::CreateInodeRequest* request,
                   pb::metaserver::CreateInodeResponse* response,
                   ::google::protobuf::Closure* done) override;
  void CreateRootInode(::google::protobuf::RpcController* controller,
                       const pb::metaserver::CreateRootInodeRequest* request,
                       pb::metaserver::CreateRootInodeResponse* response,
                       ::google::protobuf::Closure* done) override;
  void UpdateInode(::google::protobuf::RpcController* controller,
                   const pb::metaserver::UpdateInodeRequest* request,
                   pb::metaserver::UpdateInodeResponse* response,
                   ::google::protobuf::Closure* done) override;
  void DeleteInode(::google::protobuf::RpcController* controller,
                   const pb::metaserver::DeleteInodeRequest* request,
                   pb::metaserver::DeleteInodeResponse* response,
                   ::google::protobuf::Closure* done) override;
};

}  // namespace metaserver
}  // namespace dingofs
#endif  // DINGOFS_TEST_MDS_FAKE_METASERVER_H_
