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
#include <butil/strings/string_split.h>
#include <fmt/format.h>

#include <vector>

namespace dingofs {
namespace client {
namespace vfs {

static void SplitString(const std::string& str, char c,
                        std::vector<std::string>& vec) {
  butil::SplitString(str, c, &vec);
}

void CompactServiceImpl::default_method(
    google::protobuf::RpcController* controller,
    const pb::client::CompactRequest* request,
    pb::client::CompactResponse* response, google::protobuf::Closure* done) {
  (void)request;
  (void)response;
  brpc::ClosureGuard done_guard(done);
  auto* cntl = static_cast<brpc::Controller*>(controller);
  cntl->http_response().set_content_type("text/plain");

  const std::string& path = cntl->http_request().unresolved_path();

  LOG(INFO) << fmt::format("[service.compact] path: {}.", path);

  std::vector<std::string> params;
  SplitString(path, '/', params);

  butil::IOBufBuilder os;
  if (params.empty() || params.size() != 3) {
    os << "# use CompactService/[compact|try_compact]/<inode_id>/<chunk_index> "
          "to compact\n";

    os.move_to(cntl->response_attachment());
    return;
  }

  const std::string& action = params[0];
  const Ino ino = strtoull(params[1].c_str(), nullptr, 10);
  const uint32_t chunk_index = strtoull(params[2].c_str(), nullptr, 10);
  if (ino == 0) {
    os << "invalid inode id\n";
    os.move_to(cntl->response_attachment());
    return;
  }

  Status status;
  if (action == "try_compact") {
    auto ctx = std::make_shared<Context>("");

    std::vector<Slice> slices;
    uint64_t version = 0;
    status = vfs_hub_->GetMetaSystem()->ReadSlice(ctx, ino, chunk_index, 0,
                                                  &slices, version);
    if (status.ok() && !slices.empty()) {
      LOG(INFO) << fmt::format(
          "[service.compact.{}.{}] readslice finish, slice_size({}) "
          "version({}).",
          ino, chunk_index, slices.size(), version);

      std::vector<Slice> out_slices;
      status = vfs_hub_->GetCompactor()->Compact(ctx, ino, chunk_index, slices,
                                                 out_slices);
    }

  } else if (action == "compact") {
    auto ctx = std::make_shared<Context>("");
    status = vfs_hub_->GetMetaSystem()->Compact(ctx, ino, chunk_index, false);
  }

  os << fmt::format(
      "compact chunk finish, ino({}) chunk_index({}) status({}).\n", ino,
      chunk_index, status.ToString());

  os.move_to(cntl->response_attachment());
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs