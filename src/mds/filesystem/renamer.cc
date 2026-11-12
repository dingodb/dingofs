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

#include <gflags/gflags.h>

#include "mds/common/status.h"
#include "mds/filesystem/filesystem.h"

const std::string kRenameWorkerSetName = "RENAME_WORKER_SET";

DEFINE_uint32(mds_rename_worker_num, 8, "rename worker number");

namespace dingofs {
namespace mds {

template void RenameTask<FileSystem::RenameParam>::Run();

template <typename T>
void RenameTask<T>::Run() {
  RenameResult out;
  auto status = fs_->Rename(*ctx_, param_, out);

  if (cb_ != nullptr) {
    cb_(status);
  } else {
    result_ = std::move(out);
    status_ = status;
    Signal();
  }
}

bool Renamer::Init() {
  worker_set_ = ExecqWorkerSet::New(kRenameWorkerSetName, FLAGS_mds_rename_worker_num, 0, false);
  return worker_set_->Init();
}

bool Renamer::Stop() {
  if (worker_set_) worker_set_->Stop();

  return true;
}

bool Renamer::Execute(Ino new_parent, TaskRunnablePtr task) {
  if (worker_set_ == nullptr) {
    LOG(ERROR) << "renamer worker set is nullptr.";
    return false;
  }

  return worker_set_->ExecuteHash(new_parent, task);
}

}  // namespace mds
}  // namespace dingofs