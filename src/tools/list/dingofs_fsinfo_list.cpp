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

#include "tools/list/dingofs_fsinfo_list.h"

#include <google/protobuf/util/json_util.h>

DECLARE_string(mdsAddr);
DECLARE_uint32(rpcTimeoutMs);

namespace dingofs {
namespace tools {
namespace list {

using ::google::protobuf::util::JsonPrintOptions;
using ::google::protobuf::util::MessageToJsonString;

void FsInfoListTool::PrintHelp() {
  DingofsToolRpc::PrintHelp();
  std::cout << " [-mdsAddr=" << FLAGS_mdsAddr << "]"
            << " [-rpcTimeoutMs=" << FLAGS_rpcTimeoutMs << "]";
  std::cout << std::endl;
}

void FsInfoListTool::AddUpdateFlags() {
  AddUpdateFlagsFunc(dingofs::tools::SetMdsAddr);
  AddUpdateFlagsFunc(dingofs::tools::SetRpcTimeoutMs);
}

int FsInfoListTool::Init() {
  if (DingofsToolRpc::Init() != 0) {
    return -1;
  }

  dingofs::utils::SplitString(FLAGS_mdsAddr, ",", &hostsAddr_);

  service_stub_func_ =
      std::bind(&pb::mds::MdsService_Stub::ListClusterFsInfo,
                service_stub_.get(), std::placeholders::_1,
                std::placeholders::_2, std::placeholders::_3, nullptr);

  pb::mds::ListClusterFsInfoRequest request;
  AddRequest(request);

  controller_->set_timeout_ms(FLAGS_rpcTimeoutMs);

  return 0;
}

bool FsInfoListTool::AfterSendRequestToHost(const std::string& host) {
  bool ret = false;
  if (controller_->Failed()) {
    errorOutput_ << "get fsinfo from mds: " << host
                 << " failed, errorcode= " << controller_->ErrorCode()
                 << ", error text " << controller_->ErrorText() << "\n";
  } else if (show_) {
    if (response_->fsinfo().empty()) {
      std::cout << "no fs in cluster." << std::endl;
      return true;
    }

    std::string output;
    JsonPrintOptions option;
    option.add_whitespace = true;
    option.always_print_primitive_fields = true;
    option.preserve_proto_field_names = true;
    auto status = MessageToJsonString(*response_, &output, option);
    if (!status.ok()) {
      std::cout << "fail to convert fsinfo to json: " << status.message()
                << std::endl;
      return false;
    }
    std::cout << output << std::endl;

    ret = true;
  }
  return ret;
}

}  // namespace list
}  // namespace tools
}  // namespace dingofs
