/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Project: DingoFS
 * Created Date: 2024-10-28
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_METASERVER_SUPERPARTITION_SUPER_PARTITION_H_
#define CURVEFS_SRC_METASERVER_SUPERPARTITION_SUPER_PARTITION_H_

#include <glog/logging.h>

#include <memory>

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/metaserver/superpartition/super_partition_storage.h"

namespace curvefs {
namespace metaserver {
namespace superpartition {

class SuperPartition {
 public:
  explicit SuperPartition(std::shared_ptr<KVStorage> kv);

  MetaStatusCode SetFsQuota(uint32_t fs_id, const Quota& quota);

  MetaStatusCode GetFsQuota(uint32_t fs_id, Quota* quota);

  MetaStatusCode FlushFsUsage(uint32_t fs_id, const Usage& usage, Quota* quota);

  MetaStatusCode SetDirQuota(uint32_t fs_id, uint64_t dir_inode_id,
                             const Quota& quota);

  MetaStatusCode GetDirQuota(uint32_t fs_id, uint64_t dir_inode_id,
                             Quota* quota);

  MetaStatusCode DeleteDirQuota(uint32_t fs_id, uint64_t dir_inode_id);

  MetaStatusCode LoadDirQuotas(uint32_t fs_id, Quotas* quotas);

  MetaStatusCode FlushDirUsages(uint32_t fs_id, const Usages& usages);

 private:
  std::string StrErr(MetaStatusCode code);

  std::string StrQuota(const Quota& quota);

  std::string StrUsage(const Usage& usage);

  MetaStatusCode CheckQuota(Quota quota);

 private:
  std::unique_ptr<SuperPartitionStorageImpl> store_;
};

}  // namespace superpartition
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_SUPERPARTITION_SUPER_PARTITION_H_
