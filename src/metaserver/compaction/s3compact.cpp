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
 * Date: Thursday Jul 14 15:58:53 CST 2022
 * Author: wuhanqing
 */

#include "metaserver/compaction/s3compact.h"

#include <memory>

#include "metaserver/copyset/copyset_node_manager.h"

namespace dingofs {
namespace metaserver {

using pb::common::PartitionInfo;

S3Compact::S3Compact(std::shared_ptr<InodeManager> manager, PartitionInfo pinfo)
    : inodeManager(std::move(manager)), partitionInfo(std::move(pinfo)) {
  copysetNode = copyset::CopysetNodeManager::GetInstance().GetSharedCopysetNode(
      partitionInfo.poolid(), partitionInfo.copysetid());
}

}  // namespace metaserver
}  // namespace dingofs
