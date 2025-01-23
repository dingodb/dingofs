/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Created Date: 2022-03-25
 * Author: chengyi01
 */

#ifndef DINGOFS_SRC_TOOLS_QUERY_DINGOFS_INODE_S3INFOMAP_H_
#define DINGOFS_SRC_TOOLS_QUERY_DINGOFS_INODE_S3INFOMAP_H_

#include <brpc/channel.h>
#include <gflags/gflags.h>

#include <cstdint>
#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "dingofs/metaserver.pb.h"
#include "tools/dingofs_tool.h"
#include "tools/dingofs_tool_define.h"

namespace dingofs {
namespace tools {
namespace query {

using pb::metaserver::S3ChunkInfoList;

using HostAndResponseType = std::vector<
    std::pair<std::string, pb::metaserver::GetOrModifyS3ChunkInfoResponse>>;

using InodeBase = pb::metaserver::GetInodeRequest;

struct HashInodeBase {
  size_t operator()(const InodeBase& inode) const {
    auto inodeIdHash = std::hash<uint64_t>()(inode.inodeid());
    auto fsIdHash = std::hash<uint64_t>()(inode.fsid());
    return inodeIdHash ^ fsIdHash;
  }
};

struct KeyEuqalInodeBase {
  bool operator()(const InodeBase& a, const InodeBase& b) const {
    return a.fsid() == b.fsid() && a.inodeid() == b.inodeid();
  }
};

class InodeS3InfoMapTool
    : public DingofsToolRpc<pb::metaserver::GetOrModifyS3ChunkInfoRequest,
                            pb::metaserver::GetOrModifyS3ChunkInfoResponse,
                            pb::metaserver::MetaServerService_Stub> {
 public:
  explicit InodeS3InfoMapTool(const std::string& cmd = kNoInvokeCmd,
                              bool show = true)
      : DingofsToolRpc(cmd) {
    show_ = show;
  }
  void PrintHelp() override;
  int Init() override;
  std::unordered_map<InodeBase, S3ChunkInfoList, HashInodeBase,
                     KeyEuqalInodeBase>
  GetInode2S3ChunkInfoList() {
    return inode2S3ChunkInfoList_;
  }

 protected:
  void AddUpdateFlags() override;
  bool AfterSendRequestToHost(const std::string& host) override;
  bool CheckRequiredFlagDefault() override;
  void SetReceiveCallback();
  void UpdateInode2S3ChunkInfoList_(const InodeBase& inode,
                                    const S3ChunkInfoList& list);

 protected:
  std::unordered_map<InodeBase, S3ChunkInfoList, HashInodeBase,
                     KeyEuqalInodeBase>
      inode2S3ChunkInfoList_;
};

}  // namespace query
}  // namespace tools
}  // namespace dingofs

#endif  // DINGOFS_SRC_TOOLS_QUERY_DINGOFS_INODE_S3INFOMAP_H_
