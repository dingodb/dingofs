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
 * Created Date: 2022-05-25
 * Author: wanghai01
 */
#ifndef DINGOFS_SRC_TOOLS_DELETE_DINGOFS_DELETE_PARTITION_TOOL_H_
#define DINGOFS_SRC_TOOLS_DELETE_DINGOFS_DELETE_PARTITION_TOOL_H_

#include <brpc/channel.h>
#include <brpc/server.h>
#include <gflags/gflags.h>

#include <iostream>
#include <string>

#include "proto/topology.pb.h"
#include "tools/dingofs_tool.h"
#include "tools/dingofs_tool_define.h"
#include "utils/string_util.h"

namespace dingofs {
namespace tools {
namespace delete_ {

class DeletePartitionTool
    : public CurvefsToolRpc<pb::mds::topology::DeletePartitionRequest,
                            pb::mds::topology::DeletePartitionResponse,
                            pb::mds::topology::TopologyService_Stub> {
 public:
  explicit DeletePartitionTool(const std::string& cmd = kPartitionDeleteCmd)
      : CurvefsToolRpc(cmd) {}

  void PrintHelp() override;
  int Init() override;
  void InitHostsAddr() override;
  int RunCommand() override;

 protected:
  void AddUpdateFlags() override;
  bool AfterSendRequestToHost(const std::string& host) override;
  bool CheckRequiredFlagDefault() override;

 protected:
};

}  // namespace delete_
}  // namespace tools
}  // namespace dingofs

#endif  // DINGOFS_SRC_TOOLS_DELETE_DINGOFS_DELETE_PARTITION_TOOL_H_
