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
 * Created Date: 2021-12-15
 * Author: chengyi01
 */

#ifndef DINGOFS_SRC_TOOLS_STATUS_DINGOFS_STATUS_H_
#define DINGOFS_SRC_TOOLS_STATUS_DINGOFS_STATUS_H_

#include <gflags/gflags.h>

#include <iostream>
#include <memory>
#include <string>

#include "tools/dingofs_tool.h"
#include "tools/dingofs_tool_define.h"
#include "tools/status/dingofs_copyset_status.h"
#include "tools/status/dingofs_etcd_status.h"
#include "tools/status/dingofs_mds_status.h"
#include "tools/status/dingofs_metaserver_status.h"

namespace dingofs {
namespace tools {
namespace status {

class StatusTool : public DingofsTool {
 public:
  explicit StatusTool(const std::string& command = kStatusCmd)
      : DingofsTool(command) {}
  void PrintHelp() override;

  int RunCommand() override;
  int Init() override;

 protected:
  std::shared_ptr<MdsStatusTool> mdsStatusTool_;
  std::shared_ptr<MetaserverStatusTool> metaserverStatusTool_;
  std::shared_ptr<EtcdStatusTool> etcdStatusTool_;
  std::shared_ptr<CopysetStatusTool> copysetStatutsTool_;
};

}  // namespace status
}  // namespace tools
}  // namespace dingofs

#endif  // DINGOFS_SRC_TOOLS_STATUS_DINGOFS_STATUS_H_
