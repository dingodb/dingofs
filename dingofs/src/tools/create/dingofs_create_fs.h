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
 * Created Date: 2021-12-23
 * Author: chengyi01
 */

#ifndef DINGOFS_SRC_TOOLS_CREATE_DINGOFS_CREATE_FS_H_
#define DINGOFS_SRC_TOOLS_CREATE_DINGOFS_CREATE_FS_H_

#include <string>

#include "proto/mds.pb.h"
#include "tools/dingofs_tool.h"
#include "tools/dingofs_tool_define.h"

namespace dingofs {
namespace tools {
namespace create {

class CreateFsTool
    : public CurvefsToolRpc<pb::mds::CreateFsRequest, pb::mds::CreateFsResponse,
                            pb::mds::MdsService_Stub> {
 public:
  explicit CreateFsTool(const std::string& cmd = kCreateFsCmd, bool show = true)
      : CurvefsToolRpc(cmd) {
    show_ = show;
  }
  void PrintHelp() override;

  int Init() override;

  bool CheckRequiredFlagDefault() override;

 protected:
  void AddUpdateFlags() override;
  bool AfterSendRequestToHost(const std::string& host) override;
  void SetController() override;
};

}  // namespace create
}  // namespace tools
}  // namespace dingofs

#endif  // DINGOFS_SRC_TOOLS_CREATE_DINGOFS_CREATE_FS_H_
