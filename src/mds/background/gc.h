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

#ifndef DINGOFS_MDS_BACKGROUND_GC_H_
#define DINGOFS_MDS_BACKGROUND_GC_H_

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "common/blockaccess/block_accesser.h"
#include "mds/common/distribution_lock.h"
#include "mds/common/runnable.h"
#include "mds/common/status.h"
#include "mds/common/tracing.h"
#include "mds/common/type.h"
#include "mds/filesystem/filesystem.h"

namespace dingofs {
namespace mds {

class TaskMemo;
using TaskMemoSPtr = std::shared_ptr<TaskMemo>;

class CleanDelSliceTask;
using CleanDelSliceTaskSPtr = std::shared_ptr<CleanDelSliceTask>;

class CleanDelFileTask;
using CleanDelFileTaskSPtr = std::shared_ptr<CleanDelFileTask>;

class CleanFileTask;
using CleanFileTaskSPtr = std::shared_ptr<CleanFileTask>;

class CleanExpiredFileSessionTask;
using CleanExpiredFileSessionTaskSPtr = std::shared_ptr<CleanExpiredFileSessionTask>;

class GcProcessor;
using GcProcessorSPtr = std::shared_ptr<GcProcessor>;

// remember already handle task
class TaskMemo {
 public:
  TaskMemo() = default;
  ~TaskMemo() = default;

  static TaskMemoSPtr New() { return std::make_shared<TaskMemo>(); }

  bool Remember(const std::string& key) {
    utils::WriteLockGuard lg(lock_);

    return keys_.insert(key).second;
  }

  void Forget(const std::string& key) {
    utils::WriteLockGuard lg(lock_);

    keys_.erase(key);
  }

  bool Exist(const std::string& key) {
    utils::ReadLockGuard lg(lock_);

    return keys_.find(key) != keys_.end();
  }

  void Clear(const std::string& prefix) {
    utils::WriteLockGuard lg(lock_);

    for (auto it = keys_.begin(); it != keys_.end();) {
      if (it->find(prefix) == 0) {
        it = keys_.erase(it);
      } else {
        ++it;
      }
    }
  }

 private:
  utils::RWLock lock_;
  std::unordered_set<std::string> keys_;
};

// clean trash slice corresponding to s3 object
class CleanDelSliceTask : public TaskRunnable {
 public:
  CleanDelSliceTask(OperationProcessorSPtr operation_processor, blockaccess::BlockAccesserSPtr block_accessor,
                    TaskMemoSPtr task_memo, Ino ino, const std::string& key, const std::string& value)
      : operation_processor_(operation_processor),
        data_accessor_(block_accessor),
        ino_(ino),
        key_(key),
        value_(value),
        task_memo_(task_memo) {}
  ~CleanDelSliceTask() override = default;

  static CleanDelSliceTaskSPtr New(OperationProcessorSPtr operation_processor,
                                   blockaccess::BlockAccesserSPtr block_accessor, TaskMemoSPtr task_memo, Ino ino,
                                   const std::string& key, const std::string& value) {
    return std::make_shared<CleanDelSliceTask>(operation_processor, block_accessor, task_memo, ino, key, value);
  }
  std::string Type() override { return "CLEAN_DELETED_SLICE"; }

  void Run() override;

 private:
  friend class GcProcessor;

  Status CleanDelSlice();

  const Ino ino_;

  const std::string key_;
  const std::string value_;

  OperationProcessorSPtr operation_processor_;

  // data accessor for s3
  blockaccess::BlockAccesserSPtr data_accessor_;

  TaskMemoSPtr task_memo_;
};

// clean delete file corresponding to s3 object
class CleanDelFileTask : public TaskRunnable {
 public:
  CleanDelFileTask(OperationProcessorSPtr operation_processor, blockaccess::BlockAccesserSPtr block_accessor,
                   TaskMemoSPtr task_memo, const AttrEntry& attr)
      : operation_processor_(operation_processor), data_accessor_(block_accessor), task_memo_(task_memo), attr_(attr) {}
  ~CleanDelFileTask() override = default;

  static CleanDelFileTaskSPtr New(OperationProcessorSPtr operation_processor,
                                  blockaccess::BlockAccesserSPtr block_accessor, TaskMemoSPtr task_memo,
                                  const AttrEntry& attr) {
    return std::make_shared<CleanDelFileTask>(operation_processor, block_accessor, task_memo, attr);
  }

  std::string Type() override { return "CLEAN_DELETED_FILE"; }

  void Run() override;

 private:
  friend class GcProcessor;

  Status CleanDelFile(const AttrEntry& attr);

  AttrEntry attr_;

  OperationProcessorSPtr operation_processor_;

  // data accessor for s3
  blockaccess::BlockAccesserSPtr data_accessor_;

  TaskMemoSPtr task_memo_;
};

// clean file corresponding to s3 object
class CleanFileTask : public TaskRunnable {
 public:
  CleanFileTask(OperationProcessorSPtr operation_processor, blockaccess::BlockAccesserSPtr block_accessor,
                TaskMemoSPtr task_memo, const std::string& fs_name, const AttrEntry& attr)
      : operation_processor_(operation_processor),
        data_accessor_(block_accessor),
        task_memo_(task_memo),
        fs_name_(fs_name),
        attr_(attr) {}
  ~CleanFileTask() override = default;

  static CleanFileTaskSPtr New(OperationProcessorSPtr operation_processor,
                               blockaccess::BlockAccesserSPtr block_accessor, TaskMemoSPtr task_memo,
                               const std::string& fs_name, const AttrEntry& attr) {
    return std::make_shared<CleanFileTask>(operation_processor, block_accessor, task_memo, fs_name, attr);
  }

  std::string Type() override { return "CLEAN_FILE"; }

  void Run() override;

 private:
  friend class GcProcessor;

  Status CleanFile(const AttrEntry& attr);

  const std::string fs_name_;
  AttrEntry attr_;

  OperationProcessorSPtr operation_processor_;

  // data accessor for s3
  blockaccess::BlockAccesserSPtr data_accessor_;

  TaskMemoSPtr task_memo_;
};

class CleanExpiredFileSessionTask : public TaskRunnable {
 public:
  CleanExpiredFileSessionTask(OperationProcessorSPtr operation_processor, TaskMemoSPtr task_memo,
                              const std::vector<FileSessionEntry>& file_sessions)
      : operation_processor_(operation_processor), task_memo_(task_memo), file_sessions_(file_sessions) {}
  ~CleanExpiredFileSessionTask() override = default;

  static CleanExpiredFileSessionTaskSPtr New(OperationProcessorSPtr operation_processor, TaskMemoSPtr task_memo,
                                             const std::vector<FileSessionEntry>& file_sessions) {
    return std::make_shared<CleanExpiredFileSessionTask>(operation_processor, task_memo, file_sessions);
  }

  std::string Type() override { return "CLEAN_EXPIRED_FILE_SESSION"; }

  void Run() override;

 private:
  Status CleanExpiredFileSession();

  OperationProcessorSPtr operation_processor_;

  std::vector<FileSessionEntry> file_sessions_;

  TaskMemoSPtr task_memo_;
};

class GcProcessor {
 public:
  GcProcessor(FileSystemSetSPtr file_system_set, OperationProcessorSPtr operation_processor,
              DistributionLockSPtr dist_lock)
      : file_system_set_(file_system_set),
        operation_processor_(operation_processor),
        dist_lock_(dist_lock),
        task_memo_(TaskMemo::New()) {}
  ~GcProcessor() = default;

  static GcProcessorSPtr New(FileSystemSetSPtr file_system_set, OperationProcessorSPtr operation_processor,
                             DistributionLockSPtr dist_lock) {
    return std::make_shared<GcProcessor>(file_system_set, operation_processor, dist_lock);
  }

  bool Init();
  void Destroy();

  void Run();

  Status ManualCleanDelSlice(Trace& trace, uint32_t fs_id, Ino ino, uint64_t chunk_index);
  Status ManualCleanDelFile(Trace& trace, uint32_t fs_id, Ino ino);

 private:
  Status LaunchGc();

  bool Execute(TaskRunnablePtr task);
  bool Execute(Ino ino, TaskRunnablePtr task);

  Status GetClientList(std::set<std::string>& clients);
  bool HasFileSession(uint32_t fs_id, Ino ino);

  void RememberFileSessionTask(const std::vector<FileSessionEntry>& file_sessions);
  void ForgotFileSessionTask(const std::vector<FileSessionEntry>& file_sessions);

  void ScanDelSlice(const FsInfoEntry& fs_info);
  void ScanDelFile(const FsInfoEntry& fs_info);
  void ScanExpiredFileSession(const FsInfoEntry& fs_info);
  void ScanDelFs(const FsInfoEntry& fs_info);

  static bool ShouldDeleteFile(const AttrEntry& attr);
  static bool ShouldCleanFileSession(const FileSessionEntry& file_session, const std::set<std::string>& alive_clients);
  static bool ShouldRecycleFs(const FsInfoEntry& fs_info);

  void SetFsStateRecycle(const FsInfoEntry& fs_info);
  Status CleanFsInfo(const FsInfoEntry& fs_info);

  blockaccess::BlockAccesserSPtr GetOrCreateDataAccesser(uint32_t fs_id);
  blockaccess::BlockAccesserSPtr GetOrCreateDataAccesser(const FsInfoEntry& fs_info);

  std::atomic<bool> is_running_{false};

  DistributionLockSPtr dist_lock_;

  OperationProcessorSPtr operation_processor_;

  // fs_id -> data accessor
  std::map<uint32_t, blockaccess::BlockAccesserSPtr> block_accessers_;

  FileSystemSetSPtr file_system_set_;

  WorkerSetSPtr worker_set_;

  TaskMemoSPtr task_memo_;
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_BACKGROUND_GC_H_