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

#include "curvefs/src/metaserver/superpartition/super_partition.h"

#include <memory>
#include <unordered_map>

#include "curvefs/src/base/string/string.h"
#include "curvefs/src/metaserver/superpartition/access_log.h"

namespace curvefs {
namespace metaserver {
namespace superpartition {

using ::curvefs::base::string::StrFormat;
using ::curvefs::metaserver::superpartition::LogGuard;

SuperPartition::SuperPartition(std::shared_ptr<KVStorage> kv)
    : store_(std::make_unique<SuperPartitionStorageImpl>(kv)) {}

MetaStatusCode SuperPartition::SetFsQuota(uint32_t fs_id, const Quota& quota) {
  MetaStatusCode rc;
  LogGuard log([&]() {
    return StrFormat("set_fs_quota(%d,%s): %s", fs_id, StrQuota(quota),
                     StrErr(rc));
  });

  rc = CheckQuota(quota);
  if (rc == MetaStatusCode::OK) {
    rc = store_->SetFsQuota(fs_id, quota);
  }
  return rc;
}

MetaStatusCode SuperPartition::GetFsQuota(uint32_t fs_id, Quota* quota) {
  MetaStatusCode rc;
  LogGuard log([&]() {
    return StrFormat("get_fs_quota(%d): %s (%s)", fs_id, StrErr(rc),
                     StrQuota(*quota));
  });

  rc = store_->GetFsQuota(fs_id, quota);
  return rc;
}

MetaStatusCode SuperPartition::FlushFsUsage(uint32_t fs_id, const Usage& usage,
                                            Quota* quota) {
  MetaStatusCode rc;
  LogGuard log([&]() {
    return StrFormat("flush_fs_usage(%d,%s): %s (%s)", fs_id, StrUsage(usage),
                     StrErr(rc), StrQuota(*quota));
  });

  rc = store_->FlushFsUsage(fs_id, usage, quota);
  return rc;
}

MetaStatusCode SuperPartition::SetDirQuota(uint32_t fs_id,
                                           uint64_t dir_inode_id,
                                           const Quota& quota) {
  MetaStatusCode rc;
  LogGuard log([&]() {
    return StrFormat("set_dir_quota(%d,%d,%s): %s", fs_id, dir_inode_id,
                     StrQuota(quota), StrErr(rc));
  });

  rc = CheckQuota(quota);
  if (rc == MetaStatusCode::OK) {
    rc = store_->SetDirQuota(fs_id, dir_inode_id, quota);
  }
  return rc;
}

MetaStatusCode SuperPartition::GetDirQuota(uint32_t fs_id,
                                           uint64_t dir_inode_id,
                                           Quota* quota) {
  MetaStatusCode rc;
  LogGuard log([&]() {
    return StrFormat("get_dir_quota(%d,%d): %s (%s)", fs_id, dir_inode_id,
                     StrErr(rc), StrQuota(*quota));
  });

  rc = store_->GetDirQuota(fs_id, dir_inode_id, quota);
  return rc;
}

MetaStatusCode SuperPartition::DeleteDirQuota(uint32_t fs_id,
                                              uint64_t dir_inode_id) {
  MetaStatusCode rc;
  LogGuard log([&]() {
    return StrFormat("delete_dir_quota(%d,%d): %s", fs_id, dir_inode_id,
                     StrErr(rc));
  });

  rc = store_->DeleteDirQuota(fs_id, dir_inode_id);
  return rc;
}

MetaStatusCode SuperPartition::LoadDirQuotas(uint32_t fs_id, Quotas* quotas) {
  MetaStatusCode rc;
  LogGuard log([&]() {
    return StrFormat("load_dir_quotas(%d): %s (%d)", fs_id, StrErr(rc),
                     quotas->size());
  });

  rc = store_->LoadDirQuotas(fs_id, quotas);
  return rc;
}

MetaStatusCode SuperPartition::FlushDirUsages(uint32_t fs_id,
                                              const Usages& usages) {
  MetaStatusCode rc;
  LogGuard log([&]() {
    return StrFormat("flush_dir_usages(%d,%d): %s", fs_id, usages.size(),
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
  return StrFormat("[%d,%d,%d,%d]",
                   quota.has_maxbytes() ? quota.maxbytes() : -1,
                   quota.has_maxinodes() ? quota.maxinodes() : -1,
                   quota.has_usedbytes() ? quota.usedbytes() : -1,
                   quota.has_usedinodes() ? quota.usedinodes() : -1);
}

std::string SuperPartition::StrUsage(const Usage& usage) {
  return StrFormat("[%d,%d]", usage.bytes(), usage.inodes());
}

// protobuf message:
//
// message Quota {
//     optional uint64 maxBytes = 1;
//     optional uint64 maxInodes = 2;
//     optional int64 usedBytes = 3;
//     optional int64 usedInodes = 4;
// };
//
// message Usage {
//     required int64 bytes = 1;
//     required int64 inodes = 2;
// };
MetaStatusCode SuperPartition::CheckQuota(Quota quota) {
  int mask = 0x0000;
  if (quota.has_maxbytes()) {
    mask = mask | 0x1000;
  }
  if (quota.has_maxinodes()) {
    mask = mask | 0x0100;
  }
  if (quota.has_usedbytes()) {
    mask = mask | 0x0010;
  }
  if (quota.has_usedinodes()) {
    mask = mask | 0x0001;
  }

  static std::unordered_map<int, bool> valid_masks = {
      {0x1000, true},
      {0x0100, true},
      {0x1100, true},
      {0x0011, true},
  };

  if (valid_masks.find(mask) != valid_masks.end()) {
    return MetaStatusCode::OK;
  }
  return MetaStatusCode::PARAM_ERROR;
}

}  // namespace superpartition
}  // namespace metaserver
}  // namespace curvefs
