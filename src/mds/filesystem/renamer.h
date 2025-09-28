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

#ifndef DINGOFS_MDS_FILESYSTEM_RENAMER_H_
#define DINGOFS_MDS_FILESYSTEM_RENAMER_H_

#include <cstdint>
#include <memory>

#include "mds/common/context.h"
#include "mds/common/runnable.h"
#include "mds/common/status.h"

namespace dingofs {
namespace mds {

class FileSystem;
using FileSystemSPtr = std::shared_ptr<FileSystem>;

using RenameCbFunc = std::function<void(Status)>;

template <typename T>
class RenameTask : public TaskRunnable {
 public:
  RenameTask(FileSystemSPtr fs, Context* ctx, const T& param, RenameCbFunc cb)
      : fs_(fs), ctx_(ctx), param_(param), cb_(cb) {
    if (cb == nullptr) {
      cond_ = std::make_shared<BthreadCond>();
    }
  }

  ~RenameTask() override = default;

  std::string Type() override { return "RENAME"; }

  void Run() override;

  void Wait() {
    if (cond_ != nullptr) {
      cond_->IncreaseWait();
    }
  }
  void Signal() {
    if (cond_ != nullptr) {
      cond_->DecreaseSignal();
    }
  }

  Status GetStatus() { return status_; }
  uint64_t GetOldParentVersion() const { return old_parent_version_; }
  uint64_t GetNewParentVersion() const { return new_parent_version_; }

 private:
  // not delete at here
  Context* ctx_{nullptr};

  T param_;

  BthreadCondPtr cond_{nullptr};
  Status status_;
  uint64_t old_parent_version_;
  uint64_t new_parent_version_;

  RenameCbFunc cb_;
  FileSystemSPtr fs_;
};

class Renamer;
using RenamerSPtr = std::shared_ptr<Renamer>;

// Renamer is used to rename file or directory in filesystem.
// Used to execute queue run rename task
class Renamer {
 public:
  Renamer() = default;
  ~Renamer() = default;

  Renamer(const Renamer&) = delete;
  Renamer& operator=(const Renamer&) = delete;

  static RenamerSPtr New() { return std::make_shared<Renamer>(); }

  bool Init();
  bool Destroy();

  template <typename T>
  Status Execute(FileSystemSPtr fs, Context& ctx, const T& param, uint64_t& old_parent_version,
                 uint64_t& new_parent_version);

 private:
  bool Execute(TaskRunnablePtr task);

  WorkerSPtr worker_;
};

template <typename T>
Status Renamer::Execute(FileSystemSPtr fs, Context& ctx, const T& param, uint64_t& old_parent_version,
                        uint64_t& new_parent_version) {
  auto task = std::make_shared<RenameTask<T> >(fs, &ctx, param, nullptr);
  if (!Execute(task)) {
    return Status(pb::error::EINTERNAL, "commit task fail");
  }

  task->Wait();

  old_parent_version = task->GetOldParentVersion();
  new_parent_version = task->GetNewParentVersion();

  return task->GetStatus();
}

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_FILESYSTEM_RENAMER_H_
