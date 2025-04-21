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

#ifndef DINGOFS_MDSV2_BACKGROUND_FSINFO_SYNC_H_
#define DINGOFS_MDSV2_BACKGROUND_FSINFO_SYNC_H_

#include "mdsv2/common/runnable.h"
#include "mdsv2/filesystem/filesystem.h"

namespace dingofs {
namespace mdsv2 {

class FsInfoSyncTask : public TaskRunnable {
 public:
  FsInfoSyncTask(FileSystemSetSPtr file_system_set) : file_system_set_(file_system_set) {}

  ~FsInfoSyncTask() override = default;

  std::string Type() override { return "FSINFO_SYNC"; }

  void Run() override;

 private:
  FileSystemSetSPtr file_system_set_;
};

class FsInfoSync {
 public:
  FsInfoSync() = default;
  ~FsInfoSync() = default;

  bool Init();
  bool Destroy();

  static void TriggerFsInfoSync();

 private:
  bool Execute(TaskRunnablePtr task);

  WorkerSPtr worker_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDSV2_BACKGROUND_FSINFO_SYNC_H_