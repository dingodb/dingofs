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

#include <memory>
#include <string>

#include "dataaccess/accesser.h"
#include "mdsv2/common/distribution_lock.h"
#include "mdsv2/common/runnable.h"
#include "mdsv2/common/status.h"
#include "mdsv2/storage/storage.h"

namespace dingofs {
namespace mdsv2 {

class CleanDeletedSliceTask;
using CleanDeletedSliceTaskSPtr = std::shared_ptr<CleanDeletedSliceTask>;

class CleanDeletedFileTask;
using CleanDeletedFileTaskSPtr = std::shared_ptr<CleanDeletedFileTask>;

class GcProcessor;
using GcProcessorSPtr = std::shared_ptr<GcProcessor>;

class CleanDeletedSliceTask : public TaskRunnable {
 public:
  CleanDeletedSliceTask(KVStorageSPtr kv_storage,
                        dataaccess::DataAccesserPtr data_accessor,
                        const KeyValue& kv)
      : kv_storage_(kv_storage), data_accessor_(data_accessor), kv_(kv) {}
  ~CleanDeletedSliceTask() override = default;

  static CleanDeletedSliceTaskSPtr New(
      KVStorageSPtr kv_storage, dataaccess::DataAccesserPtr data_accessor,
      const KeyValue& kv) {
    return std::make_shared<CleanDeletedSliceTask>(kv_storage, data_accessor,
                                                   kv);
  }
  std::string Type() override { return "CLEAN_DELETED_SLICE"; }

  void Run() override;

 private:
  Status CleanDeletedSlice(const std::string& key, const std::string& value);

  KeyValue kv_;

  KVStorageSPtr kv_storage_;

  // data accessor for s3
  dataaccess::DataAccesserPtr data_accessor_;
};

class CleanDeletedFileTask : public TaskRunnable {
 public:
  CleanDeletedFileTask() = default;
  ~CleanDeletedFileTask() override = default;

  std::string Type() override { return "CLEAN_DELETED_FILE"; }

  void Run() override;

 private:
};

class GcProcessor {
 public:
  GcProcessor(KVStorageSPtr kv_storage, DistributionLockPtr dist_lock)
      : kv_storage_(kv_storage), dist_lock_(dist_lock) {}
  ~GcProcessor() = default;

  static GcProcessorSPtr New(KVStorageSPtr kv_storage,
                             DistributionLockPtr dist_lock) {
    return std::make_shared<GcProcessor>(kv_storage, dist_lock);
  }

  bool Init();
  void Destroy();

  void LaunchGc();

 private:
  void Execute(TaskRunnablePtr task);

  void ScanDeletedSlice();
  void ScanDeletedFile();

  std::atomic<bool> is_running_{false};

  DistributionLockPtr dist_lock_;

  KVStorageSPtr kv_storage_;

  // data accessor for s3
  dataaccess::DataAccesserPtr data_accessor_;

  WorkerSetSPtr worker_set_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDSV2_BACKGROUND_GC_H_
