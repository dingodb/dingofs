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
 * Created Date: 2021-10-30
 * Author: chengyi01
 */

#include "tools/query/dingofs_copyset_query.h"

#include "tools/copyset/dingofs_copyset_base_tool.h"

DECLARE_string(copysetId);
DECLARE_string(poolId);
DECLARE_string(mdsAddr);
DECLARE_bool(detail);

// used for getCopysetStatusTool
DECLARE_string(metaserverAddr);

namespace dingofs {
namespace tools {
namespace query {

void CopysetQueryTool::PrintHelp() {
  DingofsToolRpc::PrintHelp();
  std::cout << " -copysetId=" << FLAGS_copysetId << " -poolId=" << FLAGS_poolId
            << " [-mdsAddr=" << FLAGS_mdsAddr << "]"
            << " [-detail=" << FLAGS_detail << "]";
  std::cout << std::endl;
}

void CopysetQueryTool::AddUpdateFlags() {
  AddUpdateFlagsFunc(dingofs::tools::SetMdsAddr);
}

int CopysetQueryTool::Init() {
  if (DingofsToolRpc::Init() != 0) {
    return -1;
  }

  dingofs::utils::SplitString(FLAGS_mdsAddr, ",", &hostsAddr_);
  std::vector<std::string> copysetsId;
  dingofs::utils::SplitString(FLAGS_copysetId, ",", &copysetsId);
  std::vector<std::string> poolsId;
  dingofs::utils::SplitString(FLAGS_poolId, ",", &poolsId);
  if (copysetsId.size() != poolsId.size()) {
    std::cerr << "copysets not match pools." << std::endl;
    return -1;
  }
  pb::mds::topology::GetCopysetsInfoRequest request;
  for (unsigned i = 0; i < poolsId.size(); ++i) {
    uint64_t poolId = std::stoul(poolsId[i]);
    uint64_t copysetId = std::stoul(copysetsId[i]);
    uint64_t key = copyset::GetCopysetKey(copysetId, poolId);
    if (std::find(copysetKeys_.begin(), copysetKeys_.end(), key) !=
        copysetKeys_.end()) {  // repeat key. ignore it
      continue;
    }
    copysetKeys_.push_back(key);
    auto copysetKey = request.add_copysetkeys();
    copysetKey->set_poolid(poolId);
    copysetKey->set_copysetid(copysetId);
  }
  AddRequest(request);

  service_stub_func_ =
      std::bind(&pb::mds::topology::TopologyService_Stub::GetCopysetsInfo,
                service_stub_.get(), std::placeholders::_1,
                std::placeholders::_2, std::placeholders::_3, nullptr);
  return 0;
}

bool CopysetQueryTool::AfterSendRequestToHost(const std::string& host) {
  if (controller_->Failed()) {
    errorOutput_ << "send request\n"
                 << requestQueue_.front().DebugString() << "to mds: " << host
                 << " failed, errorcode= " << controller_->ErrorCode()
                 << ", error text " << controller_->ErrorText() << "\n";
    return false;
  } else {
    auto copysetsValue = response_->copysetvalues();
    if (static_cast<size_t>(copysetsValue.size()) != copysetKeys_.size()) {
      std::cerr << "wrong number of coysetsinfo. number of key is "
                << copysetKeys_.size() << " ,number of copysets is "
                << copysetsValue.size() << std::endl;
      return false;
    }

    for (size_t i = 0; i < copysetKeys_.size(); i++) {
      key2Info_[copysetKeys_[i]].push_back(copysetsValue[i]);
    }
    if (FLAGS_detail) {
      GetCopysetStatus();
    }

    if (show_) {
      for (auto const& i : copysetKeys_) {
        std::cout << "copyset[" << i << "]:\n[info]" << std::endl;
        if (key2Info_[i].size() != 1) {
          std::cerr << "find more or less 1 copysetInfo." << std::endl;
        }

        for (auto const& j : key2Info_[i]) {
          std::cout << j.ShortDebugString() << std::endl;
        }
        if (FLAGS_detail) {
          std::cout << "[status]" << std::endl;
          for (auto const& j : key2Status_[i]) {
            std::cout << j.ShortDebugString() << std::endl;
          }
          if (static_cast<int>(key2Status_[i].size()) !=
              key2Info_[i][0].copysetinfo().peers().size()) {
            std::cerr << "copysetStatus not match the number of "
                         "copysetInfo's peers!"
                      << std::endl;
          }
        }
      }
    }
  }
  return true;
}
bool CopysetQueryTool::GetCopysetStatus() {
  bool ret = true;
  for (auto const& i : response_->copysetvalues()) {
    using tmpType = pb::metaserver::copyset::CopysetStatusRequest;
    tmpType tmp;
    tmp.set_copysetid(i.copysetinfo().copysetid());
    tmp.set_poolid(i.copysetinfo().poolid());
    for (auto const& j : i.copysetinfo().peers()) {
      // send request to all peer
      std::string addr;
      if (!mds::topology::SplitPeerId(j.address(), &addr)) {
        std::cerr << "copyset[" << tmp.copysetid()
                  << "] has error peerid: " << j.address() << std::endl;
        ret = false;
        break;
      }
      auto& queueRequest = addr2Request_[addr];
      if (queueRequest.empty()) {
        queueRequest.push(pb::metaserver::copyset::CopysetsStatusRequest());
      }
      *queueRequest.front().add_copysets() = tmp;
    }
  }

  FLAGS_copysetId = FLAGS_poolId = "";  // clear copysetId&poolId
  for (auto const& i : addr2Request_) {
    // set host
    FLAGS_metaserverAddr = i.first;
    copyset::GetCopysetStatusTool getCopysetStatusTool("", false);
    getCopysetStatusTool.Init();
    getCopysetStatusTool.SetRequestQueue(i.second);
    auto checkRet = getCopysetStatusTool.RunCommand();
    if (checkRet < 0) {
      std::cerr << "send request to metaserver get error." << std::endl;
      ret = false;
    }
    const auto& copysetsStatus = getCopysetStatusTool.GetResponse()->status();
    auto copysets = i.second.front().copysets();
    for (int m = 0, n = 0; m < copysets.size() && n < copysetsStatus.size();
         ++m, ++n) {
      uint64_t key = (static_cast<uint64_t>(copysets[m].poolid()) << 32) |
                     copysets[m].copysetid();
      key2Status_[key].push_back(copysetsStatus[n]);
      // TODO(chengyi01): check copysetsStatus[n].status()
    }
  }
  return ret;
}

bool CopysetQueryTool::CheckRequiredFlagDefault() {
  google::CommandLineFlagInfo info;
  if (CheckPoolIdDefault(&info) && CheckCopysetIdDefault(&info)) {
    std::cerr << "no -poolId=*,* -copysetId=*,* , please use -example!"
              << std::endl;
    return true;
  }
  return false;
}

}  // namespace query
}  // namespace tools
}  // namespace dingofs
