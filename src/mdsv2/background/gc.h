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

#include "dataaccess/block_accesser.h"
#include "dingofs/mdsv2.pb.h"
#include "mdsv2/common/distribution_lock.h"
#include "mdsv2/common/runnable.h"
#include "mdsv2/common/status.h"
#include "mdsv2/common/type.h"
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
                        dataaccess::BlockAccesser* block_accesser,
                        const KeyValue& kv)
      : kv_storage_(kv_storage), block_accesser_(block_accesser), kv_(kv) {}
  ~CleanDeletedSliceTask() override = default;

  static CleanDeletedSliceTaskSPtr New(
      KVStorageSPtr kv_storage, dataaccess::BlockAccesser* block_accesser,
      const KeyValue& kv) {
    return std::make_shared<CleanDeletedSliceTask>(kv_storage, block_accesser,
                                                   kv);
  }
  std::string Type() override { return "CLEAN_DELETED_SLICE"; }

  void Run() override;

 private:
  Status CleanDeletedSlice(const std::string& key, const std::string& value);

  KeyValue kv_;

  KVStorageSPtr kv_storage_;

  // data accessor for s3
  dataaccess::BlockAccesser* block_accesser_{nullptr};
};

class CleanDeletedFileTask : public TaskRunnable {
 public:
  CleanDeletedFileTask(KVStorageSPtr kv_storage,
                       dataaccess::BlockAccesser* block_accesser,
                       const AttrType& inode)
      : kv_storage_(kv_storage),
        block_accesser_(block_accesser),
        inode_(inode) {}
  ~CleanDeletedFileTask() override = default;

  static CleanDeletedFileTaskSPtr New(KVStorageSPtr kv_storage,
                       dataaccess::BlockAccesser* block_accesser,
                                      const AttrType& inode) {
    return std::make_shared<CleanDeletedFileTask>(kv_storage, block_accesser,
                                                  inode);
  }

  std::string Type() override { return "CLEAN_DELETED_FILE"; }

  void Run() override;

 private:
  Status CleanDeletedFile(const AttrType& inode);

  AttrType inode_;

  KVStorageSPtr kv_storage_;

  // data accessor for s3
  dataaccess::BlockAccesser* block_accesser_{nullptr};
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

  void Run();

  Status LaunchGc();

 private:
  void Execute(TaskRunnablePtr task);

  void ScanDeletedSlice();
  void ScanDeletedFile();

  static bool ShouldDeleteFile(const AttrType& attr);

  std::atomic<bool> is_running_{false};

  DistributionLockPtr dist_lock_;

  KVStorageSPtr kv_storage_;

  // data accessor for s3
  std::unique_ptr<dataaccess::BlockAccesser> block_accesser_;

  WorkerSetSPtr worker_set_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDSV2_BACKGROUND_GC_H_
