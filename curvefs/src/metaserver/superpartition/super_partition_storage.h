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

#ifndef CURVEFS_SRC_METASERVER_SUPERPARTITION_SUPER_PARTITION_STORAGE_H_
#define CURVEFS_SRC_METASERVER_SUPERPARTITION_SUPER_PARTITION_STORAGE_H_

#include <glog/logging.h>

#include <memory>

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/metaserver/storage/status.h"
#include "curvefs/src/metaserver/storage/storage.h"

namespace curvefs {
namespace metaserver {
namespace superpartition {

using ::curvefs::metaserver::Quota;
using ::curvefs::metaserver::Usage;
using ::curvefs::metaserver::storage::KVStorage;
using ::curvefs::metaserver::storage::StorageTransaction;
using Quotas = ::google::protobuf::Map<uint64_t, Quota>;
using Usages = ::google::protobuf::Map<uint64_t, Usage>;
using StorageStatus = ::curvefs::metaserver::storage::Status;
using Transaction = std::shared_ptr<StorageTransaction>;

class SuperPartitionStorage {
 public:
  virtual ~SuperPartitionStorage() = default;

  // fs quota
  virtual MetaStatusCode SetFsQuota(uint32_t fs_id, const Quota& quota) = 0;

  virtual MetaStatusCode GetFsQuota(uint32_t fs_id, Quota* quota) = 0;

  virtual MetaStatusCode DeleteFsQuota(uint32_t fs_id) = 0;

  virtual MetaStatusCode FlushFsUsage(uint32_t fs_id, const Usage& usage,
                                      Quota* quota) = 0;

  // dir quota
  virtual MetaStatusCode SetDirQuota(uint32_t fs_id, uint64_t dir_inode_id,
                                     const Quota& quota) = 0;

  virtual MetaStatusCode GetDirQuota(uint32_t fs_id, uint64_t dir_inode_id,
                                     Quota* quota) = 0;

  virtual MetaStatusCode DeleteDirQuota(uint32_t fs_id,
                                        uint64_t dir_inode_id) = 0;

  virtual MetaStatusCode LoadDirQuotas(uint32_t fs_id, Quotas* quotas) = 0;

  virtual MetaStatusCode FlushDirUsages(uint32_t fs_id,
                                        const Usages& usages) = 0;
};

class SuperPartitionStorageImpl : public SuperPartitionStorage {
 public:
  explicit SuperPartitionStorageImpl(std::shared_ptr<KVStorage> kv);

  MetaStatusCode SetFsQuota(uint32_t fs_id, const Quota& quota) override;

  MetaStatusCode GetFsQuota(uint32_t fs_id, Quota* quota) override;

  MetaStatusCode DeleteFsQuota(uint32_t fs_id) override;

  MetaStatusCode FlushFsUsage(uint32_t fs_id, const Usage& usage,
                              Quota* quota) override;

  MetaStatusCode SetDirQuota(uint32_t fs_id, uint64_t dir_inode_id,
                             const Quota& quota) override;

  MetaStatusCode GetDirQuota(uint32_t fs_id, uint64_t dir_inode_id,
                             Quota* quota) override;

  MetaStatusCode DeleteDirQuota(uint32_t fs_id, uint64_t dir_inode_id) override;

  MetaStatusCode LoadDirQuotas(uint32_t fs_id, Quotas* quotas) override;

  MetaStatusCode FlushDirUsages(uint32_t fs_id, const Usages& usages) override;

 private:
  MetaStatusCode QuotaError(StorageStatus status);

  void UpdateQuota(Quota* old, const Quota& quota);

  inline std::string GetFsQuotaKey(uint32_t fs_id) const;

  inline std::string GetDirQuotaKey(uint32_t fs_id,
                                    uint64_t dir_inode_id) const;

  MetaStatusCode DoSetFsQuota(uint32_t fs_id, const Quota& quota);

  MetaStatusCode DoGetFsQuota(uint32_t fs_id, Quota* quota);

  MetaStatusCode DoDeleteFsQuota(uint32_t fs_id);

  MetaStatusCode DoSetDirQuota(uint32_t fs_id, uint64_t dir_inode_id,
                               const Quota& quota);

  MetaStatusCode DoGetDirQuota(uint32_t fs_id, uint64_t dir_inode_id,
                               Quota* quota);

  MetaStatusCode DoDeleteDirQuota(uint32_t fs_id, uint64_t dir_inode_id);

  MetaStatusCode DoFlushDirUsage(Transaction txn, uint32_t fs_id,
                                 uint64_t dir_inode_id, const Usage& usage);

 private:
  std::string fs_quota_table_;
  std::string dir_quota_table_;
  std::shared_ptr<KVStorage> kv_;
};

}  // namespace superpartition
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_SUPERPARTITION_SUPER_PARTITION_STORAGE_H_
