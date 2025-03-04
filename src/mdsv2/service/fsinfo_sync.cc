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

#include "mdsv2/service/fsinfo_sync.h"

#include "fmt/core.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/server.h"

namespace dingofs {
namespace mdsv2 {

void FsInfoSyncTask::Run() {
  DINGO_LOG(INFO) << "[fs_sync] start...";
  bool ret = file_system_set_->LoadFileSystems();
  DINGO_LOG(INFO) << fmt::format("[fs_sync] finish, load fs {}.", (ret ? "success" : "fail"));
}

bool FsInfoSync::Init() {
  worker_ = Worker::New();
  return worker_->Init();
}

bool FsInfoSync::Destroy() {
  if (worker_) {
    worker_->Destroy();
  }

  return true;
}

bool FsInfoSync::Execute(TaskRunnablePtr task) {
  if (worker_ == nullptr) {
    DINGO_LOG(ERROR) << "[fs_sync] fs info sync worker is nullptr.";
    return false;
  }
  return worker_->Execute(task);
}

void FsInfoSync::TriggerFsInfoSync() {
  auto task = std::make_shared<FsInfoSyncTask>(Server::GetInstance().GetFileSystemSet());
  Server::GetInstance().GetFsInfoSync().Execute(task);
}

}  // namespace mdsv2
}  // namespace dingofs