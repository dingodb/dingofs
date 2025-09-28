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

#ifndef DINGOFS_MDS_SERVICE_FSSTAT_H_
#define DINGOFS_MDS_SERVICE_FSSTAT_H_

#include <memory>

#include "brpc/builtin/tabbed.h"
#include "brpc/server.h"
#include "butil/iobuf.h"
#include "dingofs/web.pb.h"
#include "mds/filesystem/filesystem.h"

namespace dingofs {
namespace mds {

class FsStatServiceImpl;
using FsStatServiceImplUPtr = std::unique_ptr<FsStatServiceImpl>;

class FsStatServiceImpl : public pb::web::FsStatService, public brpc::Tabbed {
 public:
  FsStatServiceImpl() = default;

  FsStatServiceImpl(const FsStatServiceImpl&) = delete;
  FsStatServiceImpl& operator=(const FsStatServiceImpl&) = delete;

  static FsStatServiceImplUPtr New() { return std::make_unique<FsStatServiceImpl>(); }

  void default_method(::google::protobuf::RpcController* controller, const pb::web::FsStatRequest* request,
                      pb::web::FsStatResponse* response, ::google::protobuf::Closure* done) override;
  void GetTabInfo(brpc::TabInfoList*) const override;

 private:
  void RenderAutoIncrementIdGenerator(FileSystemSetSPtr file_system_set, butil::IOBufBuilder& os);
  void RenderMainPage(const brpc::Server* server, FileSystemSetSPtr file_system_set, butil::IOBufBuilder& os);
  void RenderServerPage(butil::IOBufBuilder& os);
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_SERVICE_FSSTAT_H_