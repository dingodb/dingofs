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

#include "client/vfs/metasystem/mds/executor.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

bool Executor::Init() {
  worker_set_ = mds::ExecqWorkerSet::NewUnique(
      name_, worker_num_, worker_max_pending_num_, use_pthread_);

  if (!worker_set_->Init()) {
    LOG(ERROR) << "init meta worker set fail.";
    return false;
  }

  return true;
}

void Executor::Stop() { worker_set_->Destroy(); }

bool Executor::ExecuteByHash(uint64_t hash_id, TaskRunnablePtr task,
                             bool retry) {
  do {
    if (worker_set_->ExecuteHash(hash_id, task)) {
      return true;
    }

    LOG(WARNING) << fmt::format(
        "[meta.executor] commit task fail, type({}) key({}).", task->Type(),
        task->Key());

  } while (retry);

  return false;
}

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs