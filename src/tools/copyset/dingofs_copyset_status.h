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
 * Created Date: 2021-10-31
 * Author: chengyi01
 */

#ifndef DINGOFS_SRC_TOOLS_COPYSET_DINGOFS_COPYSET_STATUS_H_
#define DINGOFS_SRC_TOOLS_COPYSET_DINGOFS_COPYSET_STATUS_H_

#include <brpc/channel.h>

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "dingofs/copyset.pb.h"
#include "tools/dingofs_tool.h"
#include "tools/dingofs_tool_define.h"
#include "utils/string_util.h"

namespace dingofs {
namespace tools {
namespace copyset {

using CopysetStatusType = pb::metaserver::copyset::CopysetStatus;

class GetCopysetStatusTool
    : public CurvefsToolRpc<pb::metaserver::copyset::CopysetsStatusRequest,
                            pb::metaserver::copyset::CopysetsStatusResponse,
                            pb::metaserver::copyset::CopysetService_Stub> {
 public:
  explicit GetCopysetStatusTool(const std::string& cmd = kNoInvokeCmd,
                                bool show = true)
      : CurvefsToolRpc(cmd) {
    show_ = show;
  }
  void PrintHelp() override;
  int RunCommand() override;
  int Init() override;
  std::map<uint64_t, std::vector<CopysetStatusType>> GetKey2CopysetStatus() {
    return key2CopysetStatus_;
  }

 protected:
  void AddUpdateFlags() override;
  bool AfterSendRequestToHost(const std::string& host) override;
  bool CheckRequiredFlagDefault() override;

 protected:
  std::vector<uint64_t> key_;
  std::map<uint64_t, std::vector<CopysetStatusType>> key2CopysetStatus_;
};

}  // namespace copyset
}  // namespace tools
}  // namespace dingofs

#endif  // DINGOFS_SRC_TOOLS_COPYSET_DINGOFS_COPYSET_STATUS_H_
