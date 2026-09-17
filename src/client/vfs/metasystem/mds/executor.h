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

using TaskRunnable = mds::TaskRunnable;
using TaskRunnablePtr = mds::TaskRunnablePtr;

// Tasks hashed to the same value are serialised, which is what keeps a per-ino
// AsyncOpen/AsyncClose pair ordered. Executor carries the background tasks
// (compact, warmup, cleanup), where the doorbell's wake cost does not matter.
class Executor {
 public:
  Executor(const std::string& name, uint32_t worker_num,
           uint32_t worker_max_pending_num)
      : name_(name),
        worker_num_(worker_num),
        worker_max_pending_num_(worker_max_pending_num) {}
  ~Executor() = default;

  bool Init();
  void Stop();

  bool ExecuteLeastQueue(TaskRunnablePtr task);
  bool ExecuteByHash(uint64_t hash_id, TaskRunnablePtr task, bool retry);

 private:
  const std::string name_;
  const uint32_t worker_num_;
  const uint32_t worker_max_pending_num_;

  mds::WorkerSetUPtr worker_set_;
};

// The open/close path. Submitting here runs on the FUSE request thread, so the
// worker set is the relay flavour: submission is a reservation plus an MPSC
// append, and a single relay thread pays the execution-queue cost off the
// request path. Both AsyncOpen and AsyncClose hash by ino into this one set,
// which is what keeps the pair ordered -- splitting them across two executors
// would let a close overtake the open that registered the session.
class FastExecutor {
 public:
  FastExecutor(const std::string& name, uint32_t worker_num)
      : name_(name), worker_num_(worker_num) {}
  ~FastExecutor() = default;

  bool Init();
  void Stop();

  bool ExecuteLeastQueue(TaskRunnablePtr task);
  bool ExecuteByHash(uint64_t hash_id, TaskRunnablePtr task, bool retry);

 private:
  const std::string name_;
  const uint32_t worker_num_;

  mds::RelayWorkerSetUPtr worker_set_;
};

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_MDS_EXECUTOR_H_