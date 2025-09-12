// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGOFS_MDSV2_SERVICE_DEBUG_H_
#define DINGOFS_MDSV2_SERVICE_DEBUG_H_

#include <cstdint>

#include "dingofs/debug.pb.h"
#include "mdsv2/filesystem/filesystem.h"

namespace dingofs {
namespace mdsv2 {

class DebugServiceImpl;
using DebugServiceImplUPtr = std::unique_ptr<DebugServiceImpl>;

class DebugServiceImpl : public pb::debug::DebugService {
 public:
  DebugServiceImpl(FileSystemSetSPtr file_system_set) : file_system_set_(file_system_set) {};
  ~DebugServiceImpl() override = default;

  DebugServiceImpl(const DebugServiceImpl&) = delete;
  DebugServiceImpl& operator=(const DebugServiceImpl&) = delete;

  static DebugServiceImplUPtr New(FileSystemSetSPtr file_system_set) {
    return std::make_unique<DebugServiceImpl>(std::move(file_system_set));
  }

  void GetLogLevel(google::protobuf::RpcController* controller, const pb::debug::GetLogLevelRequest* request,
                   pb::debug::GetLogLevelResponse* response, google::protobuf::Closure* done) override;
  void ChangeLogLevel(google::protobuf::RpcController* controller, const pb::debug::ChangeLogLevelRequest* request,
                      pb::debug::ChangeLogLevelResponse* response, google::protobuf::Closure* done) override;

  void GetFs(google::protobuf::RpcController* controller, const pb::debug::GetFsRequest* request,
             pb::debug::GetFsResponse* response, google::protobuf::Closure* done) override;

  void GetPartition(google::protobuf::RpcController* controller, const pb::debug::GetPartitionRequest* request,
                    pb::debug::GetPartitionResponse* response, google::protobuf::Closure* done) override;

  void GetInode(google::protobuf::RpcController* controller, const pb::debug::GetInodeRequest* request,
                pb::debug::GetInodeResponse* response, google::protobuf::Closure* done) override;

  void GetOpenFile(google::protobuf::RpcController* controller, const pb::debug::GetOpenFileRequest* request,
                   pb::debug::GetOpenFileResponse* response, google::protobuf::Closure* done) override;

  void TraceWorkerSet(google::protobuf::RpcController* controller, const pb::debug::TraceWorkerSetRequest* request,
                      pb::debug::TraceWorkerSetResponse* response, google::protobuf::Closure* done) override;

  void CleanCache(google::protobuf::RpcController* controller, const pb::debug::CleanCacheRequest* request,
                  pb::debug::CleanCacheResponse* response, google::protobuf::Closure* done) override;

  void GetMemoryStats(google::protobuf::RpcController* controller, const pb::debug::GetMemoryStatsRequest* request,
                      pb::debug::GetMemoryStatsResponse* response, google::protobuf::Closure* done) override;

  void ReleaseFreeMemory(google::protobuf::RpcController* controller,
                         const pb::debug::ReleaseFreeMemoryRequest* request,
                         pb::debug::ReleaseFreeMemoryResponse* response, google::protobuf::Closure* done) override;

 private:
  FileSystemSPtr GetFileSystem(uint32_t fs_id);

  FileSystemSetSPtr file_system_set_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDSV2_SERVICE_DEBUG_H_