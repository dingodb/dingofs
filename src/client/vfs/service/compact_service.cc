// Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

#include "client/vfs/service/compact_service.h"

#include <brpc/closure_guard.h>
#include <brpc/controller.h>

#include <vector>

namespace dingofs {
namespace client {
namespace vfs {

void CompactServiceImpl::default_method(
    google::protobuf::RpcController* controller,
    const pb::client::CompactRequest* request,
    pb::client::CompactResponse* response, google::protobuf::Closure* done) {
  (void)request;
  (void)response;
  brpc::ClosureGuard done_guard(done);
  auto* cntl = static_cast<brpc::Controller*>(controller);
  cntl->http_response().set_content_type("text/plain");

  butil::IOBufBuilder os;

  const std::string& path = cntl->http_request().unresolved_path();
  if (path.empty()) {
    os << "# Use CompactService/<inode_id>/<chunk_index> to compact\n";
  } else {
    char* endptr = nullptr;
    int64_t ino = strtoll(path.c_str(), &endptr, 10);
    if (*endptr != '/') {
      cntl->SetFailed(brpc::ENOMETHOD,
                      "Invalid path %s, expected /<ino>/<chunk_index>",
                      path.c_str());
      return;
    }

    int64_t chunk_index = strtoll(endptr + 1, &endptr, 10);
    if (*endptr != '\0' && *endptr != '/') {
      cntl->SetFailed(brpc::ENOMETHOD,
                      "Invalid path %s, expected /<ino>/<chunk_index>",
                      path.c_str());
      return;
    }

    {
      auto span = vfs_hub_->GetTraceManager().StartSpan("VFSWrapper::Create");
      std::vector<Slice> slices;
      uint64_t version = 0;
      vfs_hub_->GetMetaSystem()->ReadSlice(SpanScope::GetContext(span), ino,
                                           chunk_index, 0, &slices, version);
      LOG(INFO) << "ReadSlice ino: " << ino << " chunk_index: " << chunk_index
                << " slices size: " << slices.size() << " version: " << version;
      if (!slices.empty()) {
        std::vector<Slice> out_slices;
        vfs_hub_->GetCompactor()->Compact(SpanScope::GetContext(span), ino,
                                          chunk_index, slices, out_slices);
      }
    }
    LOG(INFO) << "CompactChunk ino: " << ino << " chunk_index: " << chunk_index;

    os << "CompactChunk ino: " << ino << " chunk_index: " << chunk_index
       << "\n";
  }

  os.move_to(cntl->response_attachment());
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs