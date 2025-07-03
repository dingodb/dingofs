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
 * Created Date: 2025-06-09
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_UTILS_OFFLOAD_THREAD_POOL_H_
#define DINGOFS_SRC_CACHE_UTILS_OFFLOAD_THREAD_POOL_H_

#include <functional>

#include "cache/common/type.h"
namespace dingofs {
namespace cache {

class OffloadThreadPool {
 public:
  using TaskFunc = std::function<void()>;

  static OffloadThreadPool& GetInstance() {
    static OffloadThreadPool instance;
    return instance;
  }

  void Start() { CHECK_EQ(thread_pool_.Start(32), 0); }
  void Submit(TaskFunc task) { thread_pool_.Enqueue(task); }

 private:
  TaskThreadPool thread_pool_{"offload_thread_pool"};
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_UTILS_OFFLOAD_THREAD_POOL_H_
