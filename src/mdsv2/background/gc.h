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

namespace dingofs {
namespace mdsv2 {

class CleanDelSliceTask;
using CleanDelSliceTaskSPtr = std::shared_ptr<CleanDelSliceTask>;

class CleanDelFileTask;
using CleanDelFileTaskSPtr = std::shared_ptr<CleanDelFileTask>;

class CleanExpiredFileSessionTask;
using CleanExpiredFileSessionTaskSPtr = std::shared_ptr<CleanExpiredFileSessionTask>;

class GcProcessor;
using GcProcessorSPtr = std::shared_ptr<GcProcessor>;

// clean trash slice corresponding to s3 object
class CleanDelSliceTask : public TaskRunnable {
 public:
  CleanDelSliceTask(OperationProcessorSPtr operation_processor, blockaccess::BlockAccesserSPtr block_accessor,
                    const std::string& key, const std::string& value)
      : operation_processor_(operation_processor), data_accessor_(block_accessor), key_(key), value_(value) {}
  ~CleanDelSliceTask() override = default;

  static CleanDelSliceTaskSPtr New(OperationProcessorSPtr operation_processor,
                                   blockaccess::BlockAccesserSPtr block_accessor, const std::string& key,
                                   const std::string& value) {
    return std::make_shared<CleanDelSliceTask>(operation_processor, block_accessor, key, value);
  }
  std::string Type() override { return "CLEAN_DELETED_SLICE"; }

  void Run() override;

 private:
  friend class GcProcessor;

  Status CleanDelSlice();

  const std::string key_;
  const std::string value_;

  OperationProcessorSPtr operation_processor_;

  // data accessor for s3
  blockaccess::BlockAccesserSPtr data_accessor_;
};

// clen delete file corresponding to s3 object
class CleanDelFileTask : public TaskRunnable {
 public:
  CleanDelFileTask(OperationProcessorSPtr operation_processor, blockaccess::BlockAccesserSPtr block_accessor,
                   const AttrType& attr)
      : operation_processor_(operation_processor), data_accessor_(block_accessor), attr_(attr) {}
  ~CleanDelFileTask() override = default;

  static CleanDelFileTaskSPtr New(OperationProcessorSPtr operation_processor,
                                  blockaccess::BlockAccesserSPtr block_accessor, const AttrType& attr) {
    return std::make_shared<CleanDelFileTask>(operation_processor, block_accessor, attr);
  }

  std::string Type() override { return "CLEAN_DELETED_FILE"; }

  void Run() override;

 private:
  friend class GcProcessor;

  Status CleanDelFile(const AttrType& attr);

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
  GcProcessor(FileSystemSetSPtr file_system_set, OperationProcessorSPtr operation_processor,
              DistributionLockSPtr dist_lock)
      : file_system_set_(file_system_set), operation_processor_(operation_processor), dist_lock_(dist_lock) {}
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

  void ScanDelSlice();
  void ScanDelFile();
  void ScanExpiredFileSession();

  static bool ShouldDeleteFile(const AttrType& attr);
  static bool ShouldCleanFileSession(const FileSessionEntry& file_session, const std::set<std::string>& alive_clients);

  blockaccess::BlockAccesserSPtr GetOrCreateDataAccesser(uint32_t fs_id);

  std::atomic<bool> is_running_{false};

  DistributionLockSPtr dist_lock_;

  OperationProcessorSPtr operation_processor_;

  // fs_id -> data accessor
  std::map<uint32_t, blockaccess::BlockAccesserSPtr> block_accessers_;

  FileSystemSetSPtr file_system_set_;

  WorkerSetSPtr worker_set_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDSV2_BACKGROUND_GC_H_