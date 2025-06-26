// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGOFS_MDSV2_QUOTA_QUOTA_H_
#define DINGOFS_MDSV2_QUOTA_QUOTA_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "dingofs/mdsv2.pb.h"
#include "mdsv2/common/status.h"
#include "mdsv2/common/tracing.h"
#include "mdsv2/common/type.h"
#include "mdsv2/filesystem/parent_memo.h"
#include "mdsv2/filesystem/store_operation.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace mdsv2 {
namespace quota {

class Quota {
 public:
  Quota(Ino ino, QuotaEntry quota) : ino_(ino), quota_(quota) {}
  ~Quota() = default;

  using QuotaEntry = pb::mdsv2::Quota;
  using UsageEntry = pb::mdsv2::Usage;

  Ino GetIno() const { return ino_; }

  void UpdateUsage(int64_t byte_delta, int64_t inode_delta);

  bool Check(int64_t byte_delta, int64_t inode_delta);

  UsageEntry GetUsage();
  QuotaEntry GetQuota();
  QuotaEntry GetQuotaAndDelta();

  void Refresh(const QuotaEntry& quota, const UsageEntry& minus_usage);

 private:
  Ino ino_{0};

  std::atomic<int64_t> byte_delta_{0};
  std::atomic<int64_t> inode_delta_{0};

  utils::RWLock rwlock_;
  QuotaEntry quota_;
};
using QuotaSPtr = std::shared_ptr<Quota>;

class DirQuotaMap {
 public:
  DirQuotaMap(ParentMemoSPtr parent_memo) : parent_memo_(parent_memo) {}
  ~DirQuotaMap() = default;

  void UpsertQuota(Ino ino, const QuotaEntry& quota);

  void UpdateUsage(Ino ino, int64_t byte_delta, int64_t inode_delta);

  void DeleteQuota(Ino ino);

  bool CheckQuota(Ino ino, int64_t byte_delta, int64_t inode_delta);

  QuotaSPtr GetNearestQuota(Ino ino);

  std::vector<QuotaSPtr> GetAllQuota();

  void RefreshAll(const std::unordered_map<Ino, QuotaEntry>& quota_map);

 private:
  QuotaSPtr GetQuota(Ino ino);

  ParentMemoSPtr parent_memo_;

  utils::RWLock rwlock_;
  // ino -> Quota
  std::unordered_map<Ino, QuotaSPtr> quota_map_;
};

// manages filesystem and directory quotas
// include cache and store
class QuotaManager {
 public:
  QuotaManager(uint32_t fs_id, ParentMemoSPtr parent_memo, OperationProcessorSPtr operation_processor)
      : fs_id_(fs_id),
        fs_quota_(0, {}),
        dir_quota_map_(parent_memo),
        operation_processor_(std::move(operation_processor)) {}
  ~QuotaManager() = default;

  QuotaManager(const QuotaManager&) = delete;
  QuotaManager& operator=(const QuotaManager&) = delete;
  QuotaManager(QuotaManager&&) = delete;
  QuotaManager& operator=(QuotaManager&&) = delete;

  bool Init();
  void Destroy();

  Status SetFsQuota(Trace& trace, const QuotaEntry& quota);
  Status GetFsQuota(Trace& trace, QuotaEntry& quota);
  Status DeleteFsQuota(Trace& trace);

  Status SetDirQuota(Trace& trace, Ino ino, const QuotaEntry& quota);
  Status GetDirQuota(Trace& trace, Ino ino, QuotaEntry& quota);
  Status DeleteDirQuota(Trace& trace, Ino ino);
  Status LoadDirQuotas(Trace& trace, std::map<Ino, QuotaEntry>& quota_entry_map);

  void UpdateFsUsage(int64_t byte_delta, int64_t inode_delta);
  void UpdateDirUsage(Ino parent, int64_t byte_delta, int64_t inode_delta);

  bool CheckQuota(Ino ino, int64_t byte_delta, int64_t inode_delta);
  QuotaSPtr GetNearestDirQuota(Ino ino);

  Status LoadQuota();
  Status FlushUsage();

  // Status GetActualUsage(Ino ino, UsageEntry& usage);

 private:
  Status FlushFsUsage();
  Status FlushDirUsage();

  Status LoadFsQuota();
  Status LoadAllDirQuota();

  uint32_t fs_id_;

  Quota fs_quota_;

  DirQuotaMap dir_quota_map_;

  OperationProcessorSPtr operation_processor_;
};

using QuotaManagerSPtr = std::shared_ptr<QuotaManager>;
using QuotaManagerUPtr = std::unique_ptr<QuotaManager>;

}  // namespace quota
}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDSV2_QUOTA_QUOTA_H_