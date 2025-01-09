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
 * @Project: dingo
 * @Date: 2021-09-09
 * @Author: majie1
 */

#ifndef DINGOFS_SRC_METASERVER_S3COMPACT_H_
#define DINGOFS_SRC_METASERVER_S3COMPACT_H_

#include <memory>
#include <utility>

#include "proto/common.pb.h"
#include "metaserver/inode_manager.h"

namespace dingofs {
namespace metaserver {

namespace copyset {
class CopysetNode;
}  // namespace copyset

struct S3Compact {
  S3Compact() = default;

  S3Compact(std::shared_ptr<InodeManager> manager,
            pb::common::PartitionInfo pinfo);

  S3Compact(std::shared_ptr<InodeManager> manager,
            std::shared_ptr<copyset::CopysetNode> copyset,
            pb::common::PartitionInfo pinfo)
      : inodeManager(std::move(manager)),
        copysetNode(std::move(copyset)),
        partitionInfo(std::move(pinfo)) {}

  std::shared_ptr<InodeManager> inodeManager;
  std::shared_ptr<copyset::CopysetNode> copysetNode;
  pb::common::PartitionInfo partitionInfo;
  bool canceled{false};
};

}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_SRC_METASERVER_S3COMPACT_H_
