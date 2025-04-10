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
 * Date: Wed Sep  1 17:28:51 CST 2021
 * Author: wuhanqing
 */

#include <braft/node.h>
#include <butil/status.h>

#include "dingofs/cli2.pb.h"
#include "metaserver/common/types.h"

#ifndef DINGOFS_SRC_METASERVER_COPYSET_RAFT_CLI2_H_
#define DINGOFS_SRC_METASERVER_COPYSET_RAFT_CLI2_H_

namespace dingofs {
namespace metaserver {
namespace copyset {

butil::Status GetLeader(PoolId poolId, CopysetId copysetId,
                        const braft::Configuration& conf,
                        braft::PeerId* leaderId);

}  // namespace copyset
}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_SRC_METASERVER_COPYSET_RAFT_CLI2_H_
