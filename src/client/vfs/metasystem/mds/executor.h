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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_MDS_EXECUTOR_H_
#define DINGOFS_SRC_CLIENT_VFS_META_MDS_EXECUTOR_H_

#include "mds/common/runnable.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

using WorkerSetUPtr = mds::WorkerSetUPtr;
using TaskRunnable = mds::TaskRunnable;
using TaskRunnablePtr = mds::TaskRunnablePtr;

class Executor {
 public:
  Executor(const std::string& name, uint32_t worker_num,
           uint32_t worker_max_pending_num, bool use_pthread = false)
      : name_(name),
        worker_num_(worker_num),
        worker_max_pending_num_(worker_max_pending_num),
        use_pthread_(use_pthread) {}
  ~Executor() = default;

  bool Init();
  void Stop();

  bool ExecuteByHash(uint64_t hash_id, TaskRunnablePtr task, bool retry = true);

 private:
  const std::string name_;
  const uint32_t worker_num_;
  const uint32_t worker_max_pending_num_;
  const bool use_pthread_;

  WorkerSetUPtr worker_set_;
};

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_MDS_EXECUTOR_H_