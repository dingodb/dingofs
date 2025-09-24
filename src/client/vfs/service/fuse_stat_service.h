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

#ifndef DINGOFS_SRC_CLIENT_VFS_FUSE_STAT_SERVICE_H_
#define DINGOFS_SRC_CLIENT_VFS_FUSE_STAT_SERVICE_H_

#include <glog/logging.h>

#include <memory>

#include "brpc/builtin/tabbed.h"
#include "brpc/server.h"
#include "butil/iobuf.h"
#include "client/vfs/hub/vfs_hub.h"
#include "dingofs/vfs.pb.h"
#include "dingofs/web.pb.h"

namespace dingofs {
namespace client {
namespace vfs {

struct Range {
  uint64_t start{0};
  uint64_t end{0};  // [start, end)
};
class FuseStatServiceImpl : public pb::web::FuseStatService,
                            public brpc::Tabbed {
 public:
  FuseStatServiceImpl() = default;

  FuseStatServiceImpl(const FuseStatServiceImpl&) = delete;
  FuseStatServiceImpl& operator=(const FuseStatServiceImpl&) = delete;

  void Init(VFSHub* hub) {
    CHECK_NOTNULL(hub);
    vfs_hub_ = hub;
  }

  void default_method(::google::protobuf::RpcController* controller,
                      const pb::web::FuseStatRequest* request,
                      pb::web::FuseStatResponse* response,
                      ::google::protobuf::Closure* done) override;

  void GetTabInfo(brpc::TabInfoList*) const override;

 private:
  void RenderMainPage(const brpc::Server* server, butil::IOBufBuilder& os);
  vfs::VFSHub* vfs_hub_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_FUSE_STAT_SERVICE_H_