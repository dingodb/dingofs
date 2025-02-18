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

#include "mdsv2/filesystem/filesystem.h"

namespace dingofs {
namespace mdsv2 {

void RenameTask::Run() {
  auto status = fs_->Rename(old_parent_ino_, old_name_, new_parent_ino_, new_name_);
  cb_(status);
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

bool Renamer::Execute(FileSystemPtr fs, uint64_t old_parent_ino, const std::string& old_name, uint64_t new_parent_ino,
                      const std::string& new_name, RenameCbFunc cb) {
  auto task = std::make_shared<RenameTask>(fs, old_parent_ino, old_name, new_parent_ino, new_name, cb);
  return Execute(task);
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