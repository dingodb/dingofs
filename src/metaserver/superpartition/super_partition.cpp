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

#include "metaserver/superpartition/super_partition.h"

#include <memory>
#include <unordered_map>

#include "metaserver/superpartition/access_log.h"

namespace dingofs {
namespace metaserver {
namespace superpartition {

using superpartition::LogGuard;

using pb::metaserver::MetaStatusCode;
using pb::metaserver::Quota;
using pb::metaserver::Usage;

SuperPartition::SuperPartition(std::shared_ptr<storage::KVStorage> kv)
    : store_(std::make_unique<SuperPartitionStorageImpl>(kv)) {}

MetaStatusCode SuperPartition::SetFsQuota(uint32_t fs_id, const Quota& quota) {
  pb::metaserver::MetaStatusCode rc;
  LogGuard log([&]() {
    return absl::StrFormat("set_fs_quota(%d,%s): %s", fs_id, StrQuota(quota),
                           StrErr(rc));
  });

  rc = store_->SetFsQuota(fs_id, quota);
  return rc;
}

MetaStatusCode SuperPartition::GetFsQuota(uint32_t fs_id, Quota* quota) {
  MetaStatusCode rc;
  LogGuard log([&]() {
    return absl::StrFormat("get_fs_quota(%d): %s (%s)", fs_id, StrErr(rc),
                           StrQuota(*quota));
  });

  rc = store_->GetFsQuota(fs_id, quota);
  return rc;
}

MetaStatusCode SuperPartition::DeleteFsQuota(uint32_t fs_id) {
  MetaStatusCode rc;
  LogGuard log([&]() {
    return absl::StrFormat("delete_fs_quota(%d): %s", fs_id, StrErr(rc));
  });

  rc = store_->DeleteFsQuota(fs_id);
  return rc;
}

MetaStatusCode SuperPartition::FlushFsUsage(uint32_t fs_id, const Usage& usage,
                                            Quota* quota) {
  MetaStatusCode rc;
  LogGuard log([&]() {
    return absl::StrFormat("flush_fs_usage(%d,%s): %s (%s)", fs_id,
                           StrUsage(usage), StrErr(rc), StrQuota(*quota));
  });

  rc = store_->FlushFsUsage(fs_id, usage, quota);
  return rc;
}

MetaStatusCode SuperPartition::SetDirQuota(uint32_t fs_id,
                                           uint64_t dir_inode_id,
                                           const Quota& quota) {
  MetaStatusCode rc;
  LogGuard log([&]() {
    return absl::StrFormat("set_dir_quota(%d,%d,%s): %s", fs_id, dir_inode_id,
                           StrQuota(quota), StrErr(rc));
  });

  rc = store_->SetDirQuota(fs_id, dir_inode_id, quota);
  return rc;
}

MetaStatusCode SuperPartition::GetDirQuota(uint32_t fs_id,
                                           uint64_t dir_inode_id,
                                           Quota* quota) {
  MetaStatusCode rc;
  LogGuard log([&]() {
    return absl::StrFormat("get_dir_quota(%d,%d): %s (%s)", fs_id, dir_inode_id,
                           StrErr(rc), StrQuota(*quota));
  });

  rc = store_->GetDirQuota(fs_id, dir_inode_id, quota);
  return rc;
}

MetaStatusCode SuperPartition::DeleteDirQuota(uint32_t fs_id,
                                              uint64_t dir_inode_id) {
  MetaStatusCode rc;
  LogGuard log([&]() {
    return absl::StrFormat("delete_dir_quota(%d,%d): %s", fs_id, dir_inode_id,
                           StrErr(rc));
  });

  rc = store_->DeleteDirQuota(fs_id, dir_inode_id);
  return rc;
}

MetaStatusCode SuperPartition::LoadDirQuotas(uint32_t fs_id, Quotas* quotas) {
  MetaStatusCode rc;
  LogGuard log([&]() {
    return absl::StrFormat("load_dir_quotas(%d): %s (%d)", fs_id, StrErr(rc),
                           quotas->size());
  });

  rc = store_->LoadDirQuotas(fs_id, quotas);
  return rc;
}

MetaStatusCode SuperPartition::FlushDirUsages(uint32_t fs_id,
                                              const Usages& usages) {
  MetaStatusCode rc;
  LogGuard log([&]() {
    return absl::StrFormat("flush_dir_usages(%d,%d): %s", fs_id, usages.size(),
                           StrErr(rc));
  });

  rc = store_->FlushDirUsages(fs_id, usages);
  return rc;
}

static const std::unordered_map<MetaStatusCode, std::string> kErrors = {
    {MetaStatusCode::OK, "OK"},
    {MetaStatusCode::PARAM_ERROR, "invalid argument"},
    {MetaStatusCode::NOT_FOUND, "not found"},
};

std::string SuperPartition::StrErr(MetaStatusCode code) {
  auto it = kErrors.find(code);
  if (it != kErrors.end()) {
    return it->second;
  }
  return "unknown";
}

std::string SuperPartition::StrQuota(const Quota& quota) {
  return absl::StrFormat("[%d,%d,%d,%d]",
                         quota.has_maxbytes() ? quota.maxbytes() : -1,
                         quota.has_maxinodes() ? quota.maxinodes() : -1,
                         quota.has_usedbytes() ? quota.usedbytes() : -1,
                         quota.has_usedinodes() ? quota.usedinodes() : -1);
}

std::string SuperPartition::StrUsage(const Usage& usage) {
  return absl::StrFormat("[%d,%d]", usage.bytes(), usage.inodes());
}

}  // namespace superpartition
}  // namespace metaserver
}  // namespace dingofs
