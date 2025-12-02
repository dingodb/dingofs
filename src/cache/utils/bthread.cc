/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Project: DingoFS
 * Created Date: 2025-06-06
 * Author: Jingli Chen (Wine93)
 */

#include "cache/utils/bthread.h"

#include <bthread/types.h>
#include <glog/logging.h>

#include "cache/common/macro.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

struct FuncArg {
  FuncArg(std::function<void()> func) : func(func) {}

  std::function<void()> func;
};

bthread_t RunInBthread(std::function<void()> func) {
  bthread_t tid;
  const bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
  auto* arg = new FuncArg(func);
  int rc = bthread_start_background(  // It costs about 100~200 ns
      &tid, &attr,
      [](void* arg) -> void* {
        FuncArg* func_arg = reinterpret_cast<FuncArg*>(arg);
        func_arg->func();

        delete func_arg;
        return nullptr;
      },
      (void*)arg);

  if (rc != 0) {
    LOG(ERROR) << "Start bthread failed: rc = " << rc;
    func();
    return 0;
  }

  VLOG(9) << "Start bthread success: tid = " << tid;

  return tid;
}

BthreadJoiner::BthreadJoiner() : running_(false), queue_id_({0}) {}

BthreadJoiner::~BthreadJoiner() { Shutdown(); }

Status BthreadJoiner::Start() {
  if (running_) {
    return Status::OK();
  }

  LOG(INFO) << "Bthread joiner is starting...";

  bthread::ExecutionQueueOptions queue_options;
  queue_options.use_pthread = true;
  int rc = bthread::execution_queue_start(&queue_id_, &queue_options, HandleTid,
                                          this);
  if (rc != 0) {
    LOG(ERROR) << "Start execution queue failed: rc = " << rc;
    return Status::Internal("start execution queue failed");
  }

  running_ = true;

  LOG(INFO) << "Bthread joiner is up.";

  CHECK_RUNNING("Bthread joiner");
  return Status::OK();
}

Status BthreadJoiner::Shutdown() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }

  LOG(INFO) << "Bthread joiner is shutting down...";

  if (bthread::execution_queue_stop(queue_id_) != 0) {
    LOG(ERROR) << "Stop execution queue failed.";
    return Status::Internal("stop execution queue failed");
  } else if (bthread::execution_queue_join(queue_id_) != 0) {
    LOG(ERROR) << "Join execution queue failed.";
    return Status::Internal("join execution queue failed");
  }

  LOG(INFO) << "Bthread joiner is down.";

  CHECK_DOWN("Bthread joiner");
  return Status::OK();
}

void BthreadJoiner::BackgroundJoin(bthread_t tid) {
  CHECK_RUNNING("Bthread joiner");
  CHECK_EQ(0, bthread::execution_queue_execute(queue_id_, tid));
}

int BthreadJoiner::HandleTid(void* /*meta*/,
                             bthread::TaskIterator<bthread_t>& iter) {
  for (; iter; ++iter) {
    bthread_t tid = *iter;
    int rc = bthread_join(tid, nullptr);
    if (rc != 0) {
      LOG(ERROR) << "Join bthread failed: tid = " << tid;
    } else {
      VLOG(9) << "Join bthread success: tid = " << tid;
    }
  }

  return 0;
}

}  // namespace cache
}  // namespace dingofs
