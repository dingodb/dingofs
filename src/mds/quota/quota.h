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

#ifndef DINGOFS_MDS_QUOTA_QUOTA_H_
#define DINGOFS_MDS_QUOTA_QUOTA_H_

#include <sys/types.h>

#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "mds/common/runnable.h"
#include "mds/common/status.h"
#include "mds/common/tracing.h"
#include "mds/common/type.h"
#include "mds/filesystem/fs_info.h"
#include "mds/filesystem/notify_buddy.h"
#include "mds/filesystem/parent_memo.h"
#include "mds/filesystem/store_operation.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace mds {
namespace quota {

class Quota;
using QuotaSPtr = std::shared_ptr<Quota>;

class QuotaManager;
using QuotaManagerSPtr = std::shared_ptr<QuotaManager>;
using QuotaManagerUPtr = std::unique_ptr<QuotaManager>;

class Quota {
 public:
  Quota(uint32_t fs_id, Ino ino, const QuotaEntry& quota);
  ~Quota() = default;

  static QuotaSPtr New(uint32_t fs_id, Ino ino, const QuotaEntry& quota) {
    return std::make_shared<Quota>(fs_id, ino, quota);
  }

  Ino INo() const { return ino_; }
  std::string UUID() const { return quota_.uuid(); }

  void UpdateUsage(int64_t byte_delta, int64_t inode_delta, const std::string& reason);

  bool Check(int64_t byte_delta, int64_t inode_delta);

  std::vector<UsageEntry> GetUsage();
  QuotaEntry GetQuota();
  QuotaEntry GetAccumulatedQuota();

  void Refresh(const QuotaEntry& quota, uint64_t timepoint, const std::string& reason);

  uint32_t IncNotFoundCount() { return not_found_count_++; }

 private:
  UsageEntry GetDeltaAccumulatedUsage(uint64_t& timepoint);
  UsageEntry GetTotalAccumulatedUsage(uint64_t& timepoint);
  UsageEntry CompactDeltaUsage(uint64_t timepoint);

  uint32_t fs_id_{0};
  Ino ino_{0};

  utils::RWLock rwlock_;

  QuotaEntry quota_;
  std::deque<UsageEntry> delta_usages_;

  uint64_t last_time_ns_{0};

  // store not found count, if beyond threshold then clean
  uint32_t not_found_count_{0};
};

// manage directory quotas
class DirQuotaMap {
 public:
  DirQuotaMap(uint32_t fs_id, ParentMemoSPtr parent_memo, OperationProcessorSPtr operation_processor)
      : fs_id_(fs_id), parent_memo_(parent_memo), operation_processor_(operation_processor) {}
  ~DirQuotaMap() = default;

  void UpsertQuota(Ino ino, const QuotaEntry& quota, const std::string& reason);

  void UpdateUsage(Ino ino, int64_t byte_delta, int64_t inode_delta, const std::string& reason);

  void DeleteQuota(Ino ino, const std::string& uuid);

  bool CheckQuota(Ino ino, int64_t byte_delta, int64_t inode_delta);

  QuotaSPtr GetNearestQuota(Ino ino);

  std::vector<QuotaSPtr> GetAllQuota();

  void Refresh(const std::unordered_map<Ino, QuotaEntry>& quota_map, const std::string& reason);

 private:
  QuotaSPtr GetQuota(Ino ino);
  bool GetParent(Ino ino, Ino& parent);
  bool HasQuota();

  uint32_t fs_id_{0};
  ParentMemoSPtr parent_memo_;
  OperationProcessorSPtr operation_processor_;

  utils::RWLock rwlock_;
  // ino -> Quota
  std::unordered_map<Ino, QuotaSPtr> quota_map_;
};

class UpdateDirUsageTask;
using UpdateDirUsageTaskSPtr = std::shared_ptr<UpdateDirUsageTask>;

class UpdateDirUsageTask : public TaskRunnable {
 public:
  UpdateDirUsageTask(QuotaManagerSPtr quota_manager, Ino parent, int64_t byte_delta, int64_t inode_delta,
                     const std::string& reason)
      : quota_manager_(quota_manager),
        parent_(parent),
        byte_delta_(byte_delta),
        inode_delta_(inode_delta),
        reason_(reason) {}

  ~UpdateDirUsageTask() override = default;

  static UpdateDirUsageTaskSPtr New(QuotaManagerSPtr quota_manager, Ino parent, int64_t byte_delta, int64_t inode_delta,
                                    const std::string& reason) {
    return std::make_shared<UpdateDirUsageTask>(quota_manager, parent, byte_delta, inode_delta, reason);
  }

  std::string Type() override { return "UpdateDirUsageTask"; }

  void Run() override;

 private:
  QuotaManagerSPtr quota_manager_;

  Ino parent_;
  int64_t byte_delta_;
  int64_t inode_delta_;

  const std::string reason_;
};

class DeleteDirQuotaTask;
using DeleteDirQuotaTaskSPtr = std::shared_ptr<DeleteDirQuotaTask>;

class DeleteDirQuotaTask : public TaskRunnable {
 public:
  DeleteDirQuotaTask(QuotaManagerSPtr quota_manager, Ino ino) : quota_manager_(quota_manager), ino_(ino) {}

  ~DeleteDirQuotaTask() override = default;

  static DeleteDirQuotaTaskSPtr New(QuotaManagerSPtr quota_manager, Ino ino) {
    return std::make_shared<DeleteDirQuotaTask>(quota_manager, ino);
  }

  std::string Type() override { return "DeleteDirQuotaTask"; }

  void Run() override;

 private:
  QuotaManagerSPtr quota_manager_;
  Ino ino_;
};

// manages filesystem and directory quotas
// include cache and store
class QuotaManager : public std::enable_shared_from_this<QuotaManager> {
 public:
  QuotaManager(FsInfoSPtr fs_info, ParentMemoSPtr parent_memo, OperationProcessorSPtr operation_processor,
               WorkerSetSPtr worker_set, notify::NotifyBuddySPtr notify_buddy)
      : fs_info_(fs_info),
        fs_quota_(fs_info->GetFsId(), 0, {}),
        dir_quota_map_(fs_info->GetFsId(), parent_memo, operation_processor),
        operation_processor_(std::move(operation_processor)),
        worker_set_(std::move(worker_set)),
        notify_buddy_(notify_buddy) {}
  ~QuotaManager() = default;

  QuotaManager(const QuotaManager&) = delete;
  QuotaManager& operator=(const QuotaManager&) = delete;
  QuotaManager(QuotaManager&&) = delete;
  QuotaManager& operator=(QuotaManager&&) = delete;

  static QuotaManagerSPtr New(FsInfoSPtr fs_info, ParentMemoSPtr parent_memo,
                              OperationProcessorSPtr operation_processor, WorkerSetSPtr worker_set,
                              notify::NotifyBuddySPtr notify_buddy) {
    return std::make_shared<QuotaManager>(fs_info, parent_memo, operation_processor, worker_set, notify_buddy);
  }

  QuotaManagerSPtr GetSelfPtr();

  bool Init();
  void Destroy();

  Status SetFsQuota(Trace& trace, const QuotaEntry& quota);
  Status GetFsQuota(Trace& trace, bool is_bypass_cache, QuotaEntry& quota);
  Status DeleteFsQuota(Trace& trace);

  Status SetDirQuota(Trace& trace, Ino ino, const QuotaEntry& quota, bool is_lead);
  Status GetDirQuota(Trace& trace, Ino ino, bool not_use_fs_quota, QuotaEntry& quota);
  Status DeleteDirQuota(Trace& trace, Ino ino);
  Status DeleteDirQuotaByNotified(Ino ino, const std::string& uuid);
  void AsyncDeleteDirQuota(Ino ino);
  Status LoadDirQuotas(Trace& trace, std::map<Ino, QuotaEntry>& quota_entry_map);

  void UpdateFsUsage(int64_t byte_delta, int64_t inode_delta, const std::string& reason);
  void UpdateDirUsage(Ino parent, int64_t byte_delta, int64_t inode_delta, const std::string& reason);
  void AsyncUpdateDirUsage(Ino parent, int64_t byte_delta, int64_t inode_delta, const std::string& reason);

  bool CheckQuota(Trace& trace, Ino ino, int64_t byte_delta, int64_t inode_delta);
  QuotaSPtr GetNearestDirQuota(Ino ino);

  Status LoadQuota();
  Status FlushUsage();

  // Status GetActualUsage(Ino ino, UsageEntry& usage);

 private:
  Status FlushFsUsage();
  Status FlushDirUsage();

  Status LoadFsQuota();
  Status LoadAllDirQuota();

  FsInfoSPtr fs_info_;

  Quota fs_quota_;

  DirQuotaMap dir_quota_map_;

  OperationProcessorSPtr operation_processor_;

  WorkerSetSPtr worker_set_;

  // notify buddy
  notify::NotifyBuddySPtr notify_buddy_;
};

}  // namespace quota
}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_QUOTA_QUOTA_H_