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
 * Created Date: 2022-04-29
 * Author: chengyi01
 */

#include "tools/list/dingofs_partition_list.h"

#include <json/json.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "tools/dingofs_tool_define.h"
#include "utils/string_util.h"

DECLARE_string(mdsAddr);
DECLARE_string(fsId);

namespace dingofs {
namespace tools {
namespace list {

void PartitionListTool::PrintHelp() {
  CurvefsToolRpc::PrintHelp();
  std::cout << " -fsId=" << FLAGS_fsId << " [-mdsAddr=" << FLAGS_mdsAddr << "]"
            << std::endl;
}

void PartitionListTool::AddUpdateFlags() {
  AddUpdateFlagsFunc(dingofs::tools::SetMdsAddr);
}

int PartitionListTool::Init() {
  if (CurvefsToolRpc::Init() != 0) {
    return -1;
  }

  dingofs::utils::SplitString(FLAGS_mdsAddr, ",", &hostsAddr_);
  google::CommandLineFlagInfo info;
  if (CheckFsIdDefault(&info)) {
    std::cerr << "no -fsId=*, please use --example check!" << std::endl;
    return -1;
  }

  std::vector<std::string> fsIds;
  dingofs::utils::SplitString(FLAGS_fsId, ",", &fsIds);

  service_stub_func_ =
      std::bind(&pb::mds::topology::TopologyService_Stub::ListPartition,
                service_stub_.get(), std::placeholders::_1,
                std::placeholders::_2, std::placeholders::_3, nullptr);

  pb::mds::topology::ListPartitionRequest request;
  for (const auto& i : fsIds) {
    uint32_t fsId = 0;
    dingofs::utils::StringToUl(i, &fsId);
    request.set_fsid(fsId);
    AddRequest(request);
  }

  return 0;
}

bool PartitionListTool::AfterSendRequestToHost(const std::string& host) {
  bool ret = false;
  if (controller_->Failed()) {
    errorOutput_ << "get fsinfo from mds: " << host
                 << " failed, errorcode= " << controller_->ErrorCode()
                 << ", error text " << controller_->ErrorText() << "\n";
  } else {
    if (response_->statuscode() != pb::mds::topology::TopoStatusCode::TOPO_OK) {
      std::cerr << "list partitions failed, errorcode= "
                << pb::mds::topology::TopoStatusCode_Name(
                       response_->statuscode())
                << std::endl;
    } else {
      fsId2PartitionList_[requestQueue_.front().fsid()] =
          response_->partitioninfolist();
      if (show_) {
        std::cout << response_->DebugString() << std::endl;
        ret = true;
      }
    }
  }
  return ret;
}

}  // namespace list
}  // namespace tools
}  // namespace dingofs
