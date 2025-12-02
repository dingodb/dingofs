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
 * Created Date: 2025-06-06
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_UTILS_BTHREAD_H_
#define DINGOFS_SRC_CACHE_UTILS_BTHREAD_H_

#include <bthread/bthread.h>
#include <bthread/execution_queue.h>
#include <bthread/execution_queue_inl.h>

#include <functional>
#include <memory>

#include "common/status.h"

namespace dingofs {
namespace cache {

bthread_t RunInBthread(std::function<void()> func);

class BthreadJoiner {
 public:
  BthreadJoiner();
  ~BthreadJoiner();

  Status Start();
  Status Shutdown();

  void BackgroundJoin(bthread_t tid);

 private:
  static int HandleTid(void* meta, bthread::TaskIterator<bthread_t>& iter);

  std::atomic<bool> running_;
  bthread::ExecutionQueueId<bthread_t> queue_id_;
};

using BthreadJoinerUPtr = std::unique_ptr<BthreadJoiner>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_UTILS_BTHREAD_H_
