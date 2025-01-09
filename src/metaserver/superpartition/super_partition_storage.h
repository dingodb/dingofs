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

#ifndef DINGOFS_SRC_METASERVER_SUPERPARTITION_SUPER_PARTITION_STORAGE_H_
#define DINGOFS_SRC_METASERVER_SUPERPARTITION_SUPER_PARTITION_STORAGE_H_

#include <glog/logging.h>

#include <memory>

#include "dingofs/metaserver.pb.h"
#include "metaserver/storage/status.h"
#include "metaserver/storage/storage.h"

namespace dingofs {
namespace metaserver {
namespace superpartition {

using Quotas = ::google::protobuf::Map<uint64_t, pb::metaserver::Quota>;
using Usages = ::google::protobuf::Map<uint64_t, pb::metaserver::Usage>;
using StorageStatus = metaserver::storage::Status;
using Transaction = std::shared_ptr<storage::StorageTransaction>;

class SuperPartitionStorage {
 public:
  virtual ~SuperPartitionStorage() = default;

  // fs quota
  virtual pb::metaserver::MetaStatusCode SetFsQuota(
      uint32_t fs_id, const pb::metaserver::Quota& quota) = 0;

  virtual pb::metaserver::MetaStatusCode GetFsQuota(
      uint32_t fs_id, pb::metaserver::Quota* quota) = 0;

  virtual pb::metaserver::MetaStatusCode DeleteFsQuota(uint32_t fs_id) = 0;

  virtual pb::metaserver::MetaStatusCode FlushFsUsage(
      uint32_t fs_id, const pb::metaserver::Usage& usage,
      pb::metaserver::Quota* quota) = 0;

  // dir quota
  virtual pb::metaserver::MetaStatusCode SetDirQuota(
      uint32_t fs_id, uint64_t dir_inode_id,
      const pb::metaserver::Quota& quota) = 0;

  virtual pb::metaserver::MetaStatusCode GetDirQuota(
      uint32_t fs_id, uint64_t dir_inode_id, pb::metaserver::Quota* quota) = 0;

  virtual pb::metaserver::MetaStatusCode DeleteDirQuota(
      uint32_t fs_id, uint64_t dir_inode_id) = 0;

  virtual pb::metaserver::MetaStatusCode LoadDirQuotas(uint32_t fs_id,
                                                       Quotas* quotas) = 0;

  virtual pb::metaserver::MetaStatusCode FlushDirUsages(
      uint32_t fs_id, const Usages& usages) = 0;
};

class SuperPartitionStorageImpl : public SuperPartitionStorage {
 public:
  explicit SuperPartitionStorageImpl(std::shared_ptr<storage::KVStorage> kv);

  pb::metaserver::MetaStatusCode SetFsQuota(
      uint32_t fs_id, const pb::metaserver::Quota& quota) override;

  pb::metaserver::MetaStatusCode GetFsQuota(
      uint32_t fs_id, pb::metaserver::Quota* quota) override;

  pb::metaserver::MetaStatusCode DeleteFsQuota(uint32_t fs_id) override;

  pb::metaserver::MetaStatusCode FlushFsUsage(
      uint32_t fs_id, const pb::metaserver::Usage& usage,
      pb::metaserver::Quota* quota) override;

  pb::metaserver::MetaStatusCode SetDirQuota(
      uint32_t fs_id, uint64_t dir_inode_id,
      const pb::metaserver::Quota& quota) override;

  pb::metaserver::MetaStatusCode GetDirQuota(
      uint32_t fs_id, uint64_t dir_inode_id,
      pb::metaserver::Quota* quota) override;

  pb::metaserver::MetaStatusCode DeleteDirQuota(uint32_t fs_id,
                                                uint64_t dir_inode_id) override;

  pb::metaserver::MetaStatusCode LoadDirQuotas(uint32_t fs_id,
                                               Quotas* quotas) override;

  pb::metaserver::MetaStatusCode FlushDirUsages(uint32_t fs_id,
                                                const Usages& usages) override;

 private:
  pb::metaserver::MetaStatusCode QuotaError(StorageStatus status);

  void UpdateQuota(pb::metaserver::Quota* old,
                   const pb::metaserver::Quota& quota);

  inline std::string GetFsQuotaKey(uint32_t fs_id) const;

  inline std::string GetDirQuotaKey(uint32_t fs_id,
                                    uint64_t dir_inode_id) const;

  pb::metaserver::MetaStatusCode DoSetFsQuota(
      uint32_t fs_id, const pb::metaserver::Quota& quota);

  pb::metaserver::MetaStatusCode DoGetFsQuota(uint32_t fs_id,
                                              pb::metaserver::Quota* quota);

  pb::metaserver::MetaStatusCode DoDeleteFsQuota(uint32_t fs_id);

  pb::metaserver::MetaStatusCode DoSetDirQuota(
      uint32_t fs_id, uint64_t dir_inode_id,
      const pb::metaserver::Quota& quota);

  pb::metaserver::MetaStatusCode DoGetDirQuota(uint32_t fs_id,
                                               uint64_t dir_inode_id,
                                               pb::metaserver::Quota* quota);

  pb::metaserver::MetaStatusCode DoDeleteDirQuota(uint32_t fs_id,
                                                  uint64_t dir_inode_id);

  pb::metaserver::MetaStatusCode DoFlushDirUsage(
      Transaction txn, uint32_t fs_id, uint64_t dir_inode_id,
      const pb::metaserver::Usage& usage);

  std::string fs_quota_table_;
  std::string dir_quota_table_;
  std::shared_ptr<storage::KVStorage> kv_;
};

}  // namespace superpartition
}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_SRC_METASERVER_SUPERPARTITION_SUPER_PARTITION_STORAGE_H_
