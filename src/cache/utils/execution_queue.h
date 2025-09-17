/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2025-09-17
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_UTILS_EXECUTION_QUEUE_H_
#define DINGOFS_SRC_CACHE_UTILS_EXECUTION_QUEUE_H_

#include <bthread/execution_queue.h>
#include <bthread/execution_queue_inl.h>

#include <functional>
#include <memory>

namespace dingofs {
namespace cache {

class ExecutionQueue {
 public:
  using Task = std::function<void()>;

  ExecutionQueue() = default;
  virtual ~ExecutionQueue() = default;

  void Start() {
    bthread::ExecutionQueueOptions queue_options;
    queue_options.use_pthread = true;
    CHECK_EQ(0, bthread::execution_queue_start(&queue_id_, &queue_options,
                                               HandleTask, nullptr));
  }

  void Shutdown() {
    CHECK_EQ(0, bthread::execution_queue_stop(queue_id_));
    CHECK_EQ(0, bthread::execution_queue_join(queue_id_));
  }

  void Submit(Task task) {
    CHECK_EQ(0, bthread::execution_queue_execute(queue_id_, task));
    size_.fetch_add(1);
  }

  int64_t Size() { return size_.load(); }

 private:
  static int HandleTask(void* meta, bthread::TaskIterator<Task>& iter) {
    if (iter.is_queue_stopped()) {
      return 0;
    }

    auto* self = static_cast<ExecutionQueue*>(meta);
    for (; iter; ++iter) {
      (*iter)();
      self->size_.fetch_sub(1);
    }
    return 0;
  }

  std::atomic<int64_t> size_{0};
  bthread::ExecutionQueueId<Task> queue_id_{0};
};

using ExecutionQueueSPtr = std::shared_ptr<ExecutionQueue>;

};  // namespace cache
};  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_UTILS_EXECUTION_QUEUE_H_
