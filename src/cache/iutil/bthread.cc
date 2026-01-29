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

#include "cache/iutil/bthread.h"

#include <bthread/types.h>
#include <glog/logging.h>

#include <atomic>

#include "cache/common/macro.h"

namespace dingofs {
namespace cache {
namespace iutil {

struct FuncArg {
  FuncArg(std::function<void()> func) : func(func) {}

  std::function<void()> func;
};

bthread_t RunInBthread(std::function<void()> func) {
  bthread_t tid;
  const bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
  auto* arg = new FuncArg(func);
  int rc = bthread_start_background(
      &tid, &attr,
      [](void* arg) -> void* {
        FuncArg* func_arg = reinterpret_cast<FuncArg*>(arg);
        func_arg->func();

        delete func_arg;
        return nullptr;
      },
      (void*)arg);

  if (rc != 0) {
    LOG(ERROR) << "Fail to start bthread, run in current thread";
    func();
    return 0;
  }

  VLOG(9) << "Successfully start bthread{tid=" << tid << "}";

  return tid;
}

BthreadJoiner::BthreadJoiner() : running_(false), queue_id_({0}) {}
BthreadJoiner::~BthreadJoiner() { Shutdown(); }

void BthreadJoiner::Start() {
  if (running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "BthreadJoiner is already running";
    return;
  }

  LOG(INFO) << "BthreadJoiner is starting...";

  bthread::ExecutionQueueOptions queue_options;
  queue_options.use_pthread = true;
  CHECK_EQ(0, bthread::execution_queue_start(&queue_id_, &queue_options,
                                             HandleTid, this));

  running_.store(true, std::memory_order_relaxed);
  LOG(INFO) << "Successfully start BthreadJoiner";
}

void BthreadJoiner::Shutdown() {
  if (!running_.load(std::memory_order_relaxed)) {
    return;
  }

  LOG(INFO) << "BthreadJoiner is shutting down...";

  CHECK_EQ(0, bthread::execution_queue_stop(queue_id_));
  CHECK_EQ(0, bthread::execution_queue_join(queue_id_));

  running_.store(false, std::memory_order_relaxed);
  LOG(INFO) << "Successfully shutdown BthreadJoiner";
}

void BthreadJoiner::BackgroundJoin(bthread_t tid) {
  DCHECK_RUNNING("BthreadJoiner");
  CHECK_EQ(0, bthread::execution_queue_execute(queue_id_, tid));
}

int BthreadJoiner::HandleTid(void* /*meta*/,
                             bthread::TaskIterator<bthread_t>& iter) {
  for (; iter; ++iter) {
    bthread_t tid = *iter;
    int rc = bthread_join(tid, nullptr);
    if (rc != 0) {
      LOG(ERROR) << "Fail to join bthread{tid=" << tid << "}";
    } else {
      VLOG(9) << "Successfully join bthread{tid=" << tid << "}";
    }
  }

  return 0;
}

}  // namespace iutil
}  // namespace cache
}  // namespace dingofs
