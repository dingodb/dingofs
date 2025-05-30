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

#include "mdsv2/filesystem/renamer.h"

#include "dingofs/error.pb.h"
#include "mdsv2/common/context.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/common/status.h"
#include "mdsv2/filesystem/filesystem.h"

namespace dingofs {
namespace mdsv2 {

void RenameTask::Run() {
  uint64_t old_parent_version;
  uint64_t new_parent_version;
  auto status =
      fs_->Rename(*ctx_, old_parent_, old_name_, new_parent_, new_name_, old_parent_version, new_parent_version);

  if (cb_ != nullptr) {
    cb_(status);
  } else {
    old_parent_version_ = old_parent_version;
    new_parent_version_ = new_parent_version;
    status_ = status;
    Signal();
  }
}

bool Renamer::Init() {
  worker_ = Worker::New();
  return worker_->Init();
}

bool Renamer::Destroy() {
  if (worker_) {
    worker_->Destroy();
  }

  return true;
}

Status Renamer::Execute(FileSystemSPtr fs, Context& ctx, uint64_t old_parent, const std::string& old_name,
                        uint64_t new_parent_ino, const std::string& new_name, uint64_t& old_parent_version,
                        uint64_t& new_parent_version) {
  auto task = std::make_shared<RenameTask>(fs, &ctx, old_parent, old_name, new_parent_ino, new_name, nullptr);
  bool ret = Execute(task);
  if (!ret) {
    return Status(pb::error::EINTERNAL, "commit task fail");
  }

  task->Wait();

  old_parent_version = task->GetOldParentVersion();
  new_parent_version = task->GetNewParentVersion();

  return task->GetStatus();
}

bool Renamer::Execute(TaskRunnablePtr task) {
  if (worker_ == nullptr) {
    DINGO_LOG(ERROR) << "Heartbeat worker is nullptr.";
    return false;
  }

  return worker_->Execute(task);
}

}  // namespace mdsv2
}  // namespace dingofs