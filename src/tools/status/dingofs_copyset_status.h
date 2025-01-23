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
 * Created Date: 2021-11-23
 * Author: chengyi01
 */
#ifndef DINGOFS_SRC_TOOLS_STATUS_DINGOFS_COPYSET_STATUS_H_
#define DINGOFS_SRC_TOOLS_STATUS_DINGOFS_COPYSET_STATUS_H_

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "tools/copyset/dingofs_copyset_base_tool.h"
#include "tools/dingofs_tool.h"
#include "tools/dingofs_tool_define.h"
#include "tools/list/dingofs_copysetinfo_list.h"

namespace dingofs {
namespace tools {
namespace status {

class CopysetStatusTool : public DingofsTool {
 public:
  explicit CopysetStatusTool(const std::string& command = kCopysetStatusCmd,
                             bool show = true)
      : DingofsTool(command) {
    show_ = show;
  }
  void PrintHelp() override;

  int RunCommand() override;
  int Init() override;

 protected:
  std::shared_ptr<list::CopysetInfoListTool> copyInfoListTool_;
};
}  // namespace status
}  // namespace tools
}  // namespace dingofs
#endif  // DINGOFS_SRC_TOOLS_STATUS_DINGOFS_COPYSET_STATUS_H_
