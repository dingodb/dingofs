// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "client/service/inode_objects_service.h"

#include <cstdint>

#include "brpc/closure_guard.h"
#include "brpc/controller.h"
#include "brpc/errno.pb.h"
#include "dingofs/metaserver.pb.h"
#include "client/inode_wrapper.h"
#include "client/service/flat_file.h"
#include "glog/logging.h"

namespace dingofs {
namespace client {

using pb::client::InodeObjectsRequest;
using pb::client::InodeObjectsResponse;
using pb::metaserver::Inode;

void InodeObjectsService::default_method(
    google::protobuf::RpcController* controller,
    const InodeObjectsRequest* /*request*/, InodeObjectsResponse* /*response*/,
    google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto* cntl = static_cast<brpc::Controller*>(controller);
  cntl->http_response().set_content_type("text/plain");

  butil::IOBufBuilder os;

  const std::string& path = cntl->http_request().unresolved_path();
  if (path.empty()) {
    os << "# Use inode_objects/<inode_id> to get inode objects\n";
  } else {
    char* endptr = nullptr;

    int64_t inode_id = strtoll(path.c_str(), &endptr, 10);
    if (*endptr == '\0' || *endptr == '/') {
      CHECK(inode_cache_manager_ != nullptr)
          << "inode_cache_manager_ is nullptr";

      VLOG(6) << "Get inode objects, inodeId=" << inode_id;

      std::shared_ptr<InodeWrapper> inode_wrapper;
      DINGOFS_ERROR ret =
          inode_cache_manager_->GetInode(inode_id, inode_wrapper);
      if (ret != DINGOFS_ERROR::OK) {
        LOG(INFO) << "Get inode failed, inodeId=" << inode_id;
        os << "Get inode failed, inodeId=" << inode_id << "\n";
        return;
      } else {
        Inode inode = inode_wrapper->GetInode();
        FlatFile flat_file(inode.fsid(), inode.inodeid(),
                           s3_adapter_->GetBlockSize());

        for (const auto& chunk : inode.s3chunkinfomap()) {
          uint64_t chunk_index = chunk.first;

          for (int i = 0; i < chunk.second.s3chunks_size(); i++) {
            const auto& chunk_info = chunk.second.s3chunks(i);
            VLOG(6) << "Insert chunk info, " << chunk_info.DebugString();

            flat_file.InsertChunkInfo(chunk_index, chunk_info);
          }
        }

        const std::string* delimiter =
            cntl->http_request().uri().GetQuery("delimiter");
        if (delimiter != nullptr) {
          os << flat_file.FormatStringWithHeader(true);
        } else {
          os << flat_file.FormatStringWithHeader();
        }
      }
    } else {
      LOG(INFO) << "Invalid inodeId=" << path;
      cntl->SetFailed(brpc::ENOMETHOD, "Invalid inodeId= %s", path.c_str());
    }
  }

  os.move_to(cntl->response_attachment());
}

}  // namespace client

}  // namespace dingofs