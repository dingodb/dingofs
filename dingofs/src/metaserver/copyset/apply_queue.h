/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: dingo
 * Date: Thu Sep  2 14:49:04 CST 2021
 * Author: wuhanqing
 */

#ifndef DINGOFS_SRC_METASERVER_COPYSET_APPLY_QUEUE_H_
#define DINGOFS_SRC_METASERVER_COPYSET_APPLY_QUEUE_H_

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "utils/concurrent/count_down_event.h"
#include "utils/concurrent/task_queue.h"
#include "utils/dingo_compiler_specific.h"

namespace dingofs {
namespace metaserver {
namespace copyset {

class CopysetNode;

struct ApplyQueueOption {
  uint32_t workerCount = 1;
  uint32_t queueDepth = 1;
  CopysetNode* copysetNode = nullptr;
};

class DINGO_CACHELINE_ALIGNMENT ApplyQueue {
  using TaskQueue =
      ::dingofs::utils::GenericTaskQueue<::bthread::Mutex,
                                         ::bthread::ConditionVariable>;

 public:
  ApplyQueue() : option_(), running_(false), workers_() {}

  bool Start(const ApplyQueueOption& option);

  template <typename Func, typename... Args>
  void Push(uint64_t hash, Func&& f, Args&&... args) {
    workers_[hash % option_.workerCount]->tasks.Push(
        std::forward<Func>(f), std::forward<Args>(args)...);
  }

  void Flush();

  void Stop();

 private:
  void StartWorkers();

  struct TaskWorker {
    TaskWorker(size_t cap, std::string workerName)
        : running(false),
          worker(),
          tasks(cap),
          workerName_(std::move(workerName)) {}

    void Start();

    void Stop();

    void Work();

    std::atomic<bool> running;
    std::thread worker;
    TaskQueue tasks;
    std::string workerName_;
  };

 private:
  ApplyQueueOption option_;
  std::atomic<bool> running_;
  std::vector<std::unique_ptr<TaskWorker>> workers_;
};

}  // namespace copyset
}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_SRC_METASERVER_COPYSET_APPLY_QUEUE_H_
