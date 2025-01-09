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

#include "metaserver/superpartition/super_partition_storage.h"

#include <cstdint>

#include "metaserver/storage/converter.h"

namespace dingofs {
namespace metaserver {
namespace superpartition {

using pb::metaserver::MetaStatusCode;
using pb::metaserver::Quota;
using pb::metaserver::Usage;

using storage::Key4DirQuota;
using storage::Key4FsQuota;
using storage::KVStorage;
using storage::NameGenerator;
using storage::Prefix4DirQuotas;
using storage::StorageTransaction;

SuperPartitionStorageImpl::SuperPartitionStorageImpl(
    std::shared_ptr<KVStorage> kv)
    : kv_(kv) {
  auto ng = NameGenerator(0);
  fs_quota_table_ = ng.GetFsQuotaTableName();
  dir_quota_table_ = ng.GetDirQuotaTableName();
}

// NOTE: DO NOT need any lock here
// TODO(@Wine93): independent storage for fs quota
MetaStatusCode SuperPartitionStorageImpl::SetFsQuota(uint32_t fs_id,
                                                     const Quota& quota) {
  Quota old;
  auto rc = DoGetFsQuota(fs_id, &old);
  if (rc != MetaStatusCode::OK && rc != MetaStatusCode::NOT_FOUND) {
    return rc;
  }

  UpdateQuota(&old, quota);
  return DoSetFsQuota(fs_id, old);
}

MetaStatusCode SuperPartitionStorageImpl::GetFsQuota(uint32_t fs_id,
                                                     Quota* quota) {
  return DoGetFsQuota(fs_id, quota);
}

MetaStatusCode SuperPartitionStorageImpl::DeleteFsQuota(uint32_t fs_id) {
  return DoDeleteFsQuota(fs_id);
}

MetaStatusCode SuperPartitionStorageImpl::FlushFsUsage(uint32_t fs_id,
                                                       const Usage& usage,
                                                       Quota* quota) {
  Quota old;
  auto rc = DoGetFsQuota(fs_id, &old);
  if (rc != MetaStatusCode::OK) {
    return rc;
  }

  quota->set_maxbytes(old.maxbytes());
  quota->set_maxinodes(old.maxinodes());
  quota->set_usedbytes(old.usedbytes() + usage.bytes());
  quota->set_usedinodes(old.usedinodes() + usage.inodes());
  return DoSetFsQuota(fs_id, *quota);
}

MetaStatusCode SuperPartitionStorageImpl::SetDirQuota(uint32_t fs_id,
                                                      uint64_t dir_inode_id,
                                                      const Quota& quota) {
  Quota old;
  auto rc = DoGetDirQuota(fs_id, dir_inode_id, &old);
  if (rc != MetaStatusCode::OK && rc != MetaStatusCode::NOT_FOUND) {
    return rc;
  }

  UpdateQuota(&old, quota);
  return DoSetDirQuota(fs_id, dir_inode_id, old);
}

MetaStatusCode SuperPartitionStorageImpl::GetDirQuota(uint32_t fs_id,
                                                      uint64_t dir_inode_id,
                                                      Quota* quota) {
  return DoGetDirQuota(fs_id, dir_inode_id, quota);
}

MetaStatusCode SuperPartitionStorageImpl::DeleteDirQuota(
    uint32_t fs_id, uint64_t dir_inode_id) {
  return DoDeleteDirQuota(fs_id, dir_inode_id);
}

MetaStatusCode SuperPartitionStorageImpl::LoadDirQuotas(uint32_t fs_id,
                                                        Quotas* quotas) {
  Prefix4DirQuotas prefix(fs_id);
  auto iterator = kv_->SSeek(dir_quota_table_, prefix.SerializeToString());
  if (iterator->Status() != 0) {
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
  }

  Key4DirQuota key;
  Quota quota;
  for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
    std::string skey = iterator->Key();
    std::string svalue = iterator->Value();
    if (!key.ParseFromString(skey) || !quota.ParseFromString(svalue)) {
      return MetaStatusCode::PARSE_FROM_STRING_FAILED;
    }
    quotas->insert({key.dir_inode_id, quota});
  }
  return MetaStatusCode::OK;
}

MetaStatusCode SuperPartitionStorageImpl::FlushDirUsages(uint32_t fs_id,
                                                         const Usages& usages) {
  MetaStatusCode rc;
  auto txn = kv_->BeginTransaction();
  for (const auto& item : usages) {
    auto dir_inode_id = item.first;
    auto usage = item.second;
    rc = DoFlushDirUsage(txn, fs_id, dir_inode_id, usage);
    if (rc != MetaStatusCode::OK) {
      break;
    }
  }

  if (rc == MetaStatusCode::OK) {
    rc = txn->Commit().ok() ? MetaStatusCode::OK
                            : MetaStatusCode::STORAGE_INTERNAL_ERROR;
  } else if (!txn->Rollback().ok()) {
    LOG(ERROR) << "Rollback transaction failed";
    rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;
  }
  return rc;
}

MetaStatusCode SuperPartitionStorageImpl::QuotaError(StorageStatus status) {
  if (status.ok()) {
    return MetaStatusCode::OK;
  } else if (status.IsNotFound()) {
    return MetaStatusCode::NOT_FOUND;
  }
  return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

void SuperPartitionStorageImpl::UpdateQuota(Quota* old, const Quota& quota) {
#define UPDATA_FIELD(FIELD)          \
  if (quota.has_##FIELD()) {         \
    old->set_##FIELD(quota.FIELD()); \
  } else if (!old->has_##FIELD()) {  \
    old->set_##FIELD(0);             \
  }

  UPDATA_FIELD(maxbytes);
  UPDATA_FIELD(maxinodes);
  UPDATA_FIELD(usedbytes);
  UPDATA_FIELD(usedinodes);

#undef UPDATA_FIELD
}

std::string SuperPartitionStorageImpl::GetFsQuotaKey(uint32_t fs_id) const {
  return Key4FsQuota(fs_id).SerializeToString();
}

std::string SuperPartitionStorageImpl::GetDirQuotaKey(
    uint32_t fs_id, uint64_t dir_inode_id) const {
  return Key4DirQuota(fs_id, dir_inode_id).SerializeToString();
}

MetaStatusCode SuperPartitionStorageImpl::DoSetFsQuota(uint32_t fs_id,
                                                       const Quota& quota) {
  return QuotaError(kv_->SSet(fs_quota_table_, GetFsQuotaKey(fs_id), quota));
}

MetaStatusCode SuperPartitionStorageImpl::DoGetFsQuota(uint32_t fs_id,
                                                       Quota* quota) {
  return QuotaError(kv_->SGet(fs_quota_table_, GetFsQuotaKey(fs_id), quota));
}

MetaStatusCode SuperPartitionStorageImpl::DoDeleteFsQuota(uint32_t fs_id) {
  return QuotaError(kv_->SDel(fs_quota_table_, GetFsQuotaKey(fs_id)));
}

MetaStatusCode SuperPartitionStorageImpl::DoSetDirQuota(uint32_t fs_id,
                                                        uint64_t dir_inode_id,
                                                        const Quota& quota) {
  return QuotaError(
      kv_->SSet(dir_quota_table_, GetDirQuotaKey(fs_id, dir_inode_id), quota));
}

MetaStatusCode SuperPartitionStorageImpl::DoGetDirQuota(uint32_t fs_id,
                                                        uint64_t dir_inode_id,
                                                        Quota* quota) {
  return QuotaError(
      kv_->SGet(dir_quota_table_, GetDirQuotaKey(fs_id, dir_inode_id), quota));
}

MetaStatusCode SuperPartitionStorageImpl::DoDeleteDirQuota(
    uint32_t fs_id, uint64_t dir_inode_id) {
  return QuotaError(
      kv_->SDel(dir_quota_table_, GetDirQuotaKey(fs_id, dir_inode_id)));
}

MetaStatusCode SuperPartitionStorageImpl::DoFlushDirUsage(Transaction txn,
                                                          uint32_t fs_id,
                                                          uint64_t dir_inode_id,
                                                          const Usage& usage) {
  Quota old;
  auto rc = QuotaError(
      txn->SGet(dir_quota_table_, GetDirQuotaKey(fs_id, dir_inode_id), &old));
  if (rc != MetaStatusCode::OK) {
    return rc;
  }

  uint64_t used_bytes = old.usedbytes() + usage.bytes();
  uint64_t used_inodes = old.usedinodes() + usage.inodes();
  old.set_usedbytes(used_bytes);
  old.set_usedinodes(used_inodes);
  return QuotaError(
      txn->SSet(dir_quota_table_, GetDirQuotaKey(fs_id, dir_inode_id), old));
}

}  // namespace superpartition
}  // namespace metaserver
}  // namespace dingofs
