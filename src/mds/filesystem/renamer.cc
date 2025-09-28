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

#include "mds/filesystem/renamer.h"

#include "mds/common/logging.h"
#include "mds/common/status.h"
#include "mds/filesystem/filesystem.h"

namespace dingofs {
namespace mds {

template void RenameTask<FileSystem::RenameParam>::Run();

template <typename T>
void RenameTask<T>::Run() {
  uint64_t old_parent_version;
  uint64_t new_parent_version;
  auto status = fs_->Rename(*ctx_, param_, old_parent_version, new_parent_version);

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

bool Renamer::Execute(TaskRunnablePtr task) {
  if (worker_ == nullptr) {
    DINGO_LOG(ERROR) << "renamer worker is nullptr.";
    return false;
  }

  return worker_->Execute(task);
}

}  // namespace mds
}  // namespace dingofs