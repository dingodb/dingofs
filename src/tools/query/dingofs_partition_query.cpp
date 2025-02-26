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
 * Created Date: 2021-11-19
 * Author: chengyi01
 */

#include "tools/query/dingofs_partition_query.h"

DECLARE_string(mdsAddr);
DECLARE_string(partitionId);

namespace dingofs {
namespace tools {
namespace query {

void PartitionQueryTool::PrintHelp() {
  DingofsToolRpc::PrintHelp();
  std::cout << " -partitionId=" << FLAGS_partitionId
            << " [-mdsAddr=" << FLAGS_mdsAddr << "]";
  std::cout << std::endl;
}

int PartitionQueryTool::Init() {
  if (DingofsToolRpc::Init() != 0) {
    return -1;
  }

  dingofs::utils::SplitString(FLAGS_mdsAddr, ",", &hostsAddr_);

  std::vector<std::string> partitionId;
  dingofs::utils::SplitString(FLAGS_partitionId, ",", &partitionId);
  pb::mds::topology::GetCopysetOfPartitionRequest request;
  for (auto const& i : partitionId) {
    request.add_partitionid(std::stoul(i));
  }
  AddRequest(request);

  service_stub_func_ =
      std::bind(&pb::mds::topology::TopologyService_Stub::GetCopysetOfPartition,
                service_stub_.get(), std::placeholders::_1,
                std::placeholders::_2, std::placeholders::_3, nullptr);

  return 0;
}

void PartitionQueryTool::AddUpdateFlags() {
  AddUpdateFlagsFunc(dingofs::tools::SetMdsAddr);
}

bool PartitionQueryTool::AfterSendRequestToHost(const std::string& host) {
  if (controller_->Failed()) {
    errorOutput_ << "send query partition request\n"
                 << requestQueue_.front().DebugString() << "\nto mds: " << host
                 << " failed, errorcode= " << controller_->ErrorCode()
                 << ", error text " << controller_->ErrorText() << std::endl;
    return false;
  } else if (response_->statuscode() != pb::mds::topology::TOPO_OK) {
    std::cerr << "query partition [" << FLAGS_partitionId
              << "] error, error code=" << response_->statuscode()
              << " error name is "
              << pb::mds::topology::TopoStatusCode_Name(response_->statuscode())
              << std::endl;
  } else if (show_) {
    std::cout << response_->DebugString() << std::endl;
  }
  return true;
}

bool PartitionQueryTool::CheckRequiredFlagDefault() {
  google::CommandLineFlagInfo info;
  if (CheckPartitionIdDefault(&info)) {
    std::cerr << "no -partitionId=***, please use -example!" << std::endl;
    return true;
  }
  return false;
}

}  // namespace query
}  // namespace tools
}  // namespace dingofs
