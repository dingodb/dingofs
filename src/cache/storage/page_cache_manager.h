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
 * Created Date: 2025-03-17
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_UTILS_PAGE_CACHE_MANAGER_H_
#define DINGOFS_SRC_CACHE_UTILS_PAGE_CACHE_MANAGER_H_

#include <bthread/execution_queue.h>
#include <sys/types.h>

#include "cache/utils/context.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

class PageCacheManager {
 public:
  PageCacheManager();

  Status Start();
  Status Shutdown();

  void AsyncDropPageCache(ContextSPtr ctx, int fd, off_t offset, size_t length,
                          bool sync = false);

 private:
  struct Task {  // drop task
    Task(ContextSPtr ctx, int fd, off_t offset, size_t length, bool sync)
        : ctx(ctx), fd(fd), offset(offset), length(length), sync(sync) {}

    ContextSPtr ctx;
    int fd;
    off_t offset;
    size_t length;
    bool sync;
  };

  static int HandleTask(void* meta, bthread::TaskIterator<Task>& iter);
  void Handle(const Task& task);

  void SyncData(int fd);
  void DropCache(int fd, off_t offset, size_t length);
  void CloseFd(int fd);

  std::atomic<bool> running_;
  bthread::ExecutionQueueId<Task> queue_id_;
};

using PageCacheManagerUPtr = std::unique_ptr<PageCacheManager>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_UTILS_PAGE_CACHE_MANAGER_H_
