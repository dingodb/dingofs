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

#include "tools/delete/dingofs_delete_partition_tool.h"

DECLARE_string(partitionId);
DECLARE_string(mdsAddr);
DECLARE_bool(noconfirm);

namespace dingofs {
namespace tools {
namespace delete_ {

void DeletePartitionTool::PrintHelp() {
  DingofsToolRpc::PrintHelp();
  std::cout << " -partitionId=" << FLAGS_partitionId
            << " [-noconfirm=" << FLAGS_noconfirm
            << "] [-mdsAddr=" << FLAGS_mdsAddr << "]";
  std::cout << std::endl;
}

int DeletePartitionTool::Init() {
  int ret = DingofsToolRpc::Init();

  pb::mds::topology::DeletePartitionRequest request;
  request.set_partitionid(std::stoul(FLAGS_partitionId));
  AddRequest(request);
  service_stub_func_ =
      std::bind(&pb::mds::topology::TopologyService_Stub::DeletePartition,
                service_stub_.get(), std::placeholders::_1,
                std::placeholders::_2, std::placeholders::_3, nullptr);
  return ret;
}

void DeletePartitionTool::InitHostsAddr() {
  dingofs::utils::SplitString(FLAGS_mdsAddr, ",", &hostsAddr_);
}

void DeletePartitionTool::AddUpdateFlags() {
  AddUpdateFlagsFunc(dingofs::tools::SetMdsAddr);
}

int DeletePartitionTool::RunCommand() {
  if (!FLAGS_noconfirm) {
    // confirm input
    std::cout << "This command will delete partition (" << FLAGS_partitionId
              << ") and is not recoverable!!!\n"
              << "Do you really want to delete this fs? [Yes, delete!]: ";
    std::string confirm = "no";
    std::getline(std::cin, confirm);
    if (confirm != "Yes, delete!") {
      // input is not 'Y' or 'y'
      std::cout << "delete canceled!" << std::endl;
      return -1;
    }
  }
  return DingofsToolRpc::RunCommand();
}

bool DeletePartitionTool::AfterSendRequestToHost(const std::string& host) {
  bool ret = false;
  if (controller_->Failed()) {
    errorOutput_ << "send delete partition request to mds: " << host
                 << " failed. errorcode= " << controller_->ErrorCode()
                 << ", error text " << controller_->ErrorText() << "\n";
  } else if (response_->statuscode() ==
             pb::mds::topology::TopoStatusCode::TOPO_OK) {
    std::cout << "delete partition (" << FLAGS_partitionId << ") success."
              << std::endl;
    ret = true;
  } else {
    std::cerr << "delete partition from mds: " << host
              << " failed. errorcode= " << response_->statuscode()
              << ", errorname: "
              << pb::mds::topology::TopoStatusCode_Name(response_->statuscode())
              << std::endl;
  }
  return ret;
}

bool DeletePartitionTool::CheckRequiredFlagDefault() {
  google::CommandLineFlagInfo info;
  if (CheckPartitionIdDefault(&info)) {
    std::cerr << "no -partitionId=***, please use -example!" << std::endl;
    return true;
  }
  return false;
}

}  // namespace delete_
}  // namespace tools
}  // namespace dingofs
