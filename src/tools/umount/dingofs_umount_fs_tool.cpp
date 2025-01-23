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
 * Created Date: 2021-09-14
 * Author: chengyi01
 */
#include "tools/umount/dingofs_umount_fs_tool.h"

#include "utils/string_util.h"

DECLARE_string(fsName);
DECLARE_string(mountpoint);
DECLARE_string(confPath);
DECLARE_string(mdsAddr);

namespace dingofs {
namespace tools {
namespace umount {

using pb::mds::Mountpoint;

void UmountFsTool::PrintHelp() {
  DingofsToolRpc::PrintHelp();
  std::cout << " -fsName=" << FLAGS_fsName
            << " -mountpoint=" << FLAGS_mountpoint
            << " [-mdsAddr=" << FLAGS_mdsAddr << "]";
  std::cout << std::endl;
}

int UmountFsTool::RunCommand() {
  // call system umount
  std::ostringstream cmd;
  std::string::size_type pos = FLAGS_mountpoint.find(':');
  if (pos == std::string::npos) {
    std::cerr << "mountpoint " << FLAGS_mountpoint << " is invalid.\n"
              << std::endl;
    return -1;
  }
  std::string localPath(FLAGS_mountpoint.begin() + pos + 1,
                        FLAGS_mountpoint.end());
  cmd << "umount " << localPath;
  int ret = 0;
  try {
    std::string command = cmd.str();
    std::thread sysUmount(std::system, command.c_str());

    // umount from cluster
    ret = DingofsToolRpc::RunCommand();
    sysUmount.join();
  } catch (std::exception& e) {
    std::cerr << "system umount " << localPath
              << " failed, error info: " << e.what() << std::endl;
    ret = -1;
  }
  return ret;
}

int UmountFsTool::Init() {
  int ret = DingofsToolRpc::Init();

  // adjust the unique element of the queue
  pb::mds::UmountFsRequest request;
  request.set_fsname(FLAGS_fsName);
  // set mountpoint
  std::vector<std::string> mountpoint;
  utils::SplitString(FLAGS_mountpoint, ":", &mountpoint);
  uint32_t port = 0;
  if (mountpoint.size() < 3 || !utils::StringToUl(mountpoint[1], &port)) {
    std::cerr << "mountpoint " << FLAGS_mountpoint << " is invalid.\n"
              << std::endl;
    return -1;
  }
  auto* mp = new Mountpoint();
  mp->set_hostname(mountpoint[0]);
  mp->set_port(port);
  mp->set_path(mountpoint[2]);
  request.set_allocated_mountpoint(mp);
  AddRequest(request);

  service_stub_func_ =
      std::bind(&pb::mds::MdsService_Stub::UmountFs, service_stub_.get(),
                std::placeholders::_1, std::placeholders::_2,
                std::placeholders::_3, nullptr);
  return ret;
}

void UmountFsTool::InitHostsAddr() {
  utils::SplitString(FLAGS_mdsAddr, ",", &hostsAddr_);
}

void UmountFsTool::AddUpdateFlags() { AddUpdateFlagsFunc(tools::SetMdsAddr); }

bool UmountFsTool::AfterSendRequestToHost(const std::string& host) {
  bool ret = false;
  if (controller_->Failed()) {
    errorOutput_ << "send umount fs request " << FLAGS_mountpoint
                 << " to mds: " << host
                 << " failed, errorcode= " << controller_->ErrorCode()
                 << ", error text " << controller_->ErrorText() << std::endl;
  } else {
    switch (response_->statuscode()) {
      case pb::mds::FSStatusCode::OK:
        std::cout << "umount fs from cluster success." << std::endl;
        ret = true;
        break;
      case pb::mds::FSStatusCode::MOUNT_POINT_NOT_EXIST:
        std::cerr << "there is no mountpoint [" << FLAGS_mountpoint
                  << "] in cluster." << std::endl;
        break;
      case pb::mds::FSStatusCode::NOT_FOUND:
        std::cerr << "there is no fsName [" << FLAGS_fsName << "] in cluster."
                  << std::endl;
        break;
      case pb::mds::FSStatusCode::FS_BUSY:
        std::cerr << "the fs [" << FLAGS_fsName
                  << "] is in use, please check or try again." << std::endl;
        break;
      default:
        std::cerr << "umount fs from mds: " << host << " fail, error code is "
                  << response_->statuscode() << " code name is :"
                  << pb::mds::FSStatusCode_Name(response_->statuscode())
                  << std::endl;
        break;
    }
  }
  return ret;
}

bool UmountFsTool::CheckRequiredFlagDefault() {
  google::CommandLineFlagInfo info;
  if (CheckFsNameDefault(&info)) {
    std::cerr << "no -fsName=***, please use -example!" << std::endl;
    return true;
  }
  return false;
}

}  // namespace umount
}  // namespace tools
}  // namespace dingofs
