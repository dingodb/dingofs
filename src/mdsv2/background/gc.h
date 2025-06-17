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

#ifndef DINGOFS_MDSV2_BACKGROUND_GC_H_
#define DINGOFS_MDSV2_BACKGROUND_GC_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "blockaccess/block_accesser.h"
#include "mdsv2/common/distribution_lock.h"
#include "mdsv2/common/runnable.h"
#include "mdsv2/common/status.h"
#include "mdsv2/common/tracing.h"
#include "mdsv2/common/type.h"
#include "mdsv2/filesystem/filesystem.h"
#include "mdsv2/storage/storage.h"

namespace dingofs {
namespace mdsv2 {

class CleanDeletedSliceTask;
using CleanDeletedSliceTaskSPtr = std::shared_ptr<CleanDeletedSliceTask>;

class CleanDeletedFileTask;
using CleanDeletedFileTaskSPtr = std::shared_ptr<CleanDeletedFileTask>;

class CleanExpiredFileSessionTask;
using CleanExpiredFileSessionTaskSPtr = std::shared_ptr<CleanExpiredFileSessionTask>;

class GcProcessor;
using GcProcessorSPtr = std::shared_ptr<GcProcessor>;

// clean trash slice corresponding to s3 object
class CleanDeletedSliceTask : public TaskRunnable {
 public:
  CleanDeletedSliceTask(OperationProcessorSPtr operation_processor, blockaccess::BlockAccesserSPtr block_accessor,
                        const KeyValue& kv)
      : operation_processor_(operation_processor), data_accessor_(block_accessor), kv_(kv) {}
  ~CleanDeletedSliceTask() override = default;

  static CleanDeletedSliceTaskSPtr New(OperationProcessorSPtr operation_processor,
                                       blockaccess::BlockAccesserSPtr block_accessor, const KeyValue& kv) {
    return std::make_shared<CleanDeletedSliceTask>(operation_processor, block_accessor, kv);
  }
  std::string Type() override { return "CLEAN_DELETED_SLICE"; }

  void Run() override;

 private:
  friend class GcProcessor;

  Status CleanDeletedSlice();

  KeyValue kv_;

  OperationProcessorSPtr operation_processor_;

  // data accessor for s3
  blockaccess::BlockAccesserSPtr data_accessor_;
};

// clen delete file corresponding to s3 object
class CleanDeletedFileTask : public TaskRunnable {
 public:
  CleanDeletedFileTask(OperationProcessorSPtr operation_processor, blockaccess::BlockAccesserSPtr block_accessor,
                       const AttrType& attr)
      : operation_processor_(operation_processor), data_accessor_(block_accessor), attr_(attr) {}
  ~CleanDeletedFileTask() override = default;

  static CleanDeletedFileTaskSPtr New(OperationProcessorSPtr operation_processor,
                                      blockaccess::BlockAccesserSPtr block_accessor, const AttrType& attr) {
    return std::make_shared<CleanDeletedFileTask>(operation_processor, block_accessor, attr);
  }

  std::string Type() override { return "CLEAN_DELETED_FILE"; }

  void Run() override;

 private:
  friend class GcProcessor;

  Status CleanDeletedFile(const AttrType& attr);

  AttrType attr_;

  OperationProcessorSPtr operation_processor_;

  // data accessor for s3
  blockaccess::BlockAccesserSPtr data_accessor_;
};

class CleanExpiredFileSessionTask : public TaskRunnable {
 public:
  CleanExpiredFileSessionTask(OperationProcessorSPtr operation_processor,
                              const std::vector<FileSessionEntry>& file_sessions)
      : operation_processor_(operation_processor), file_sessions_(file_sessions) {}
  ~CleanExpiredFileSessionTask() override = default;

  static CleanExpiredFileSessionTaskSPtr New(OperationProcessorSPtr operation_processor,
                                             const std::vector<FileSessionEntry>& file_sessions) {
    return std::make_shared<CleanExpiredFileSessionTask>(operation_processor, file_sessions);
  }

  std::string Type() override { return "CLEAN_EXPIRED_FILE_SESSION"; }

  void Run() override;

 private:
  Status CleanExpiredFileSession();

  OperationProcessorSPtr operation_processor_;

  std::vector<FileSessionEntry> file_sessions_;
};

class GcProcessor {
 public:
  GcProcessor(FileSystemSetSPtr file_system_set, KVStorageSPtr kv_storage, OperationProcessorSPtr operation_processor,
              DistributionLockSPtr dist_lock)
      : file_system_set_(file_system_set),
        kv_storage_(kv_storage),
        operation_processor_(operation_processor),
        dist_lock_(dist_lock) {}
  ~GcProcessor() = default;

  static GcProcessorSPtr New(FileSystemSetSPtr file_system_set, KVStorageSPtr kv_storage,
                             OperationProcessorSPtr operation_processor, DistributionLockSPtr dist_lock) {
    return std::make_shared<GcProcessor>(file_system_set, kv_storage, operation_processor, dist_lock);
  }

  bool Init();
  void Destroy();

  void Run();

  Status ManualCleanDeletedSlice(Trace& trace, uint32_t fs_id, Ino ino, uint64_t chunk_index);
  Status ManualCleanDeletedFile(Trace& trace, uint32_t fs_id, Ino ino);

 private:
  Status LaunchGc();

  void Execute(TaskRunnablePtr task);
  void Execute(int64_t id, TaskRunnablePtr task);

  void ScanDeletedSlice();
  void ScanDeletedFile();
  void ScanExpiredFileSession();

  static bool ShouldDeleteFile(const AttrType& attr);
  static bool ShouldCleanFileSession(const FileSessionEntry& file_session);

  blockaccess::BlockAccesserSPtr GetOrCreateDataAccesser(uint32_t fs_id);

  std::atomic<bool> is_running_{false};

  DistributionLockSPtr dist_lock_;

  KVStorageSPtr kv_storage_;

  OperationProcessorSPtr operation_processor_;

  // fs_id -> data accessor
  std::map<uint32_t, blockaccess::BlockAccesserSPtr> block_accessers_;

  FileSystemSetSPtr file_system_set_;

  WorkerSetSPtr worker_set_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDSV2_BACKGROUND_GC_H_