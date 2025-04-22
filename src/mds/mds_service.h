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

#ifndef DINGOFS_SRC_MDS_MDS_SERVICE_H_
#define DINGOFS_SRC_MDS_MDS_SERVICE_H_

#include <brpc/closure_guard.h>
#include <brpc/controller.h>

#include <memory>
#include <string>
#include <utility>

#include "dingofs/mds.pb.h"
#include "mds/common/mds_define.h"
#include "mds/fs_manager.h"
#include "mds/idgenerator/chunkid_allocator.h"

namespace dingofs {
namespace mds {

class MdsServiceImpl : public pb::mds::MdsService {
 public:
  MdsServiceImpl(std::shared_ptr<FsManager> fsManager,
                 std::shared_ptr<ChunkIdAllocator> chunkIdAllocator)
      : fsManager_(std::move(fsManager)),
        chunkIdAllocator_(std::move(chunkIdAllocator)) {}

  void CreateFs(::google::protobuf::RpcController* controller,
                const pb::mds::CreateFsRequest* request,
                pb::mds::CreateFsResponse* response,
                ::google::protobuf::Closure* done) override;

  void MountFs(::google::protobuf::RpcController* controller,
               const pb::mds::MountFsRequest* request,
               pb::mds::MountFsResponse* response,
               ::google::protobuf::Closure* done) override;

  void UmountFs(::google::protobuf::RpcController* controller,
                const pb::mds::UmountFsRequest* request,
                pb::mds::UmountFsResponse* response,
                ::google::protobuf::Closure* done) override;

  void GetFsInfo(::google::protobuf::RpcController* controller,
                 const pb::mds::GetFsInfoRequest* request,
                 pb::mds::GetFsInfoResponse* response,
                 ::google::protobuf::Closure* done) override;

  void DeleteFs(::google::protobuf::RpcController* controller,
                const pb::mds::DeleteFsRequest* request,
                pb::mds::DeleteFsResponse* response,
                ::google::protobuf::Closure* done) override;

  void AllocateS3Chunk(::google::protobuf::RpcController* controller,
                       const pb::mds::AllocateS3ChunkRequest* request,
                       pb::mds::AllocateS3ChunkResponse* response,
                       ::google::protobuf::Closure* done) override;

  void ListClusterFsInfo(::google::protobuf::RpcController* controller,
                         const pb::mds::ListClusterFsInfoRequest* request,
                         pb::mds::ListClusterFsInfoResponse* response,
                         ::google::protobuf::Closure* done) override;

  void RefreshSession(::google::protobuf::RpcController* controller,
                      const pb::mds::RefreshSessionRequest* request,
                      pb::mds::RefreshSessionResponse* response,
                      ::google::protobuf::Closure* done) override;

  void GetLatestTxId(::google::protobuf::RpcController* controller,
                     const pb::mds::GetLatestTxIdRequest* request,
                     pb::mds::GetLatestTxIdResponse* response,
                     ::google::protobuf::Closure* done) override;

  void CommitTx(::google::protobuf::RpcController* controller,
                const pb::mds::CommitTxRequest* request,
                pb::mds::CommitTxResponse* response,
                ::google::protobuf::Closure* done) override;

  void SetFsStats(::google::protobuf::RpcController* controller,
                  const pb::mds::SetFsStatsRequest* request,
                  pb::mds::SetFsStatsResponse* response,
                  ::google::protobuf::Closure* done);

  void GetFsStats(::google::protobuf::RpcController* controller,
                  const pb::mds::GetFsStatsRequest* request,
                  pb::mds::GetFsStatsResponse* response,
                  ::google::protobuf::Closure* done);

  void GetFsPerSecondStats(::google::protobuf::RpcController* controller,
                           const pb::mds::GetFsPerSecondStatsRequest* request,
                           pb::mds::GetFsPerSecondStatsResponse* response,
                           ::google::protobuf::Closure* done);

 private:
  std::shared_ptr<FsManager> fsManager_;
  std::shared_ptr<ChunkIdAllocator> chunkIdAllocator_;
};
}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_SRC_MDS_MDS_SERVICE_H_
