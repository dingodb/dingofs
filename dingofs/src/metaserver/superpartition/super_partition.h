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

#ifndef DINGOFS_SRC_METASERVER_SUPERPARTITION_SUPER_PARTITION_H_
#define DINGOFS_SRC_METASERVER_SUPERPARTITION_SUPER_PARTITION_H_

#include <glog/logging.h>

#include <memory>

#include "proto/metaserver.pb.h"
#include "metaserver/superpartition/super_partition_storage.h"

namespace dingofs {
namespace metaserver {
namespace superpartition {

class SuperPartition {
 public:
  explicit SuperPartition(std::shared_ptr<storage::KVStorage> kv);

  pb::metaserver::MetaStatusCode SetFsQuota(uint32_t fs_id,
                                            const pb::metaserver::Quota& quota);

  pb::metaserver::MetaStatusCode GetFsQuota(uint32_t fs_id,
                                            pb::metaserver::Quota* quota);

  pb::metaserver::MetaStatusCode DeleteFsQuota(uint32_t fs_id);

  pb::metaserver::MetaStatusCode FlushFsUsage(
      uint32_t fs_id, const pb::metaserver::Usage& usage,
      pb::metaserver::Quota* quota);

  pb::metaserver::MetaStatusCode SetDirQuota(
      uint32_t fs_id, uint64_t dir_inode_id,
      const pb::metaserver::Quota& quota);

  pb::metaserver::MetaStatusCode GetDirQuota(uint32_t fs_id,
                                             uint64_t dir_inode_id,
                                             pb::metaserver::Quota* quota);

  pb::metaserver::MetaStatusCode DeleteDirQuota(uint32_t fs_id,
                                                uint64_t dir_inode_id);

  pb::metaserver::MetaStatusCode LoadDirQuotas(uint32_t fs_id, Quotas* quotas);

  pb::metaserver::MetaStatusCode FlushDirUsages(uint32_t fs_id,
                                                const Usages& usages);

 private:
  std::string StrErr(pb::metaserver::MetaStatusCode code);

  std::string StrQuota(const pb::metaserver::Quota& quota);

  std::string StrUsage(const pb::metaserver::Usage& usage);

  std::unique_ptr<SuperPartitionStorageImpl> store_;
};

}  // namespace superpartition
}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_SRC_METASERVER_SUPERPARTITION_SUPER_PARTITION_H_
