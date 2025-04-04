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
 * Created Date: 2021-10-29
 * Author: chengyi01
 */
#ifndef DINGOFS_SRC_TOOLS_STATUS_DINGOFS_METASERVER_STATUS_H_
#define DINGOFS_SRC_TOOLS_STATUS_DINGOFS_METASERVER_STATUS_H_

#include <string>
#include <vector>

#include "tools/dingofs_tool_define.h"
#include "tools/status/dingofs_status_base_tool.h"
#include "utils/string_util.h"

namespace dingofs {
namespace tools {
namespace status {

class MetaserverStatusTool : public StatusBaseTool {
 public:
  explicit MetaserverStatusTool(
      const std::string& cmd = kMetaserverStatusCmd,
      const std::string& hostType = kHostTypeMetaserver)
      : StatusBaseTool(cmd, hostType) {}
  void PrintHelp() override;
  void InitHostsAddr() override;
  int Init() override;

 protected:
  void AddUpdateFlags() override;
  int ProcessMetrics() override;
  void AfterGetMetric(const std::string hostAddr, const std::string& subUri,
                      const std::string& value,
                      const MetricStatusCode& statusCode) override;
};

}  // namespace status
}  // namespace tools
}  // namespace dingofs

#endif  // DINGOFS_SRC_TOOLS_STATUS_DINGOFS_METASERVER_STATUS_H_
