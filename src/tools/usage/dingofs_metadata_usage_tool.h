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
 * Created Date: 2021-10-22
 * Author: chengyi01
 */

#ifndef DINGOFS_SRC_TOOLS_USAGE_DINGOFS_METADATA_USAGE_TOOL_H_
#define DINGOFS_SRC_TOOLS_USAGE_DINGOFS_METADATA_USAGE_TOOL_H_

#include <functional>
#include <iostream>
#include <string>
#include <vector>

#include "dingofs/space.pb.h"
#include "dingofs/topology.pb.h"
#include "tools/dingofs_tool.h"
#include "tools/dingofs_tool_define.h"
#include "tools/usage/dingofs_space_base_tool.h"
#include "utils/string_util.h"

namespace dingofs {
namespace tools {
namespace usage {

/**
 * @brief  this class is used to query the metadata usage of cluster
 *
 * @details
 */
class MatedataUsageTool
    : public CurvefsToolRpc<pb::mds::topology::StatMetadataUsageRequest,
                            pb::mds::topology::StatMetadataUsageResponse,
                            pb::mds::topology::TopologyService_Stub> {
 public:
  explicit MatedataUsageTool(const std::string& cmd = kMetedataUsageCmd)
      : CurvefsToolRpc(cmd) {}
  void PrintHelp() override;

 protected:
  void AddUpdateFlags() override;
  bool AfterSendRequestToHost(const std::string& host) override;

 private:
  int Init() override;
};

}  // namespace usage
}  // namespace tools
}  // namespace dingofs

#endif  // DINGOFS_SRC_TOOLS_USAGE_DINGOFS_METADATA_USAGE_TOOL_H_
