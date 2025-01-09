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
#ifndef DINGOFS_SRC_TOOLS_COPYSET_DINGOFS_COPYSET_BASE_TOOL_H_
#define DINGOFS_SRC_TOOLS_COPYSET_DINGOFS_COPYSET_BASE_TOOL_H_

#include <map>
#include <queue>
#include <string>
#include <vector>

#include "dingofs/copyset.pb.h"
#include "dingofs/mds.pb.h"
#include "dingofs/topology.pb.h"
#include "mds/topology/deal_peerid.h"
#include "tools/copyset/dingofs_copyset_status.h"

namespace dingofs {
namespace tools {
namespace copyset {

uint64_t GetCopysetKey(uint64_t copysetId, uint64_t poolId);

bool CopysetInfo2CopysetStatus(
    const pb::mds::topology::GetCopysetsInfoResponse& response,
    std::map<uint64_t,
             std::vector<pb::metaserver::copyset::CopysetStatusResponse>>*
        key2Status);

bool CopysetInfo2CopysetStatus(
    const pb::mds::topology::ListCopysetInfoResponse& response,
    std::map<uint64_t,
             std::vector<pb::metaserver::copyset::CopysetStatusResponse>>*
        key2Status);

bool Response2CopysetInfo(
    const pb::mds::topology::GetCopysetsInfoResponse& response,
    std::map<uint64_t, std::vector<pb::mds::topology::CopysetValue>>* key2Info);

bool Response2CopysetInfo(
    const pb::mds::topology::ListCopysetInfoResponse& response,
    std::map<uint64_t, std::vector<pb::mds::topology::CopysetValue>>* key2Info);

enum class CheckResult {
  kHealthy = 0,
  // the number of copysetInfo is greater than 1
  kOverCopyset = -1,
  // the number of copysetInfo is less than 1
  kNoCopyset = -2,
  // copyset topo is not ok
  kTopoNotOk = -3,
  // peer not match
  kPeersNoSufficient = -4,
  // peer op status not health
  kPeerOpNotOk = -5,
};

CheckResult checkCopysetHelthy(
    const std::vector<pb::mds::topology::CopysetValue>& copysetInfoVec,
    const std::vector<pb::metaserver::copyset::CopysetStatusResponse>&
        copysetStatusVec);

}  // namespace copyset
}  // namespace tools
}  // namespace dingofs

#endif  // DINGOFS_SRC_TOOLS_COPYSET_DINGOFS_COPYSET_BASE_TOOL_H_
