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

#include "cache/common/common.h"

namespace dingofs {
namespace cache {
namespace utils {

class PageCacheManager {
 public:
  PageCacheManager();

  Status Start();

  Status Stop();

  void DropPageCache(int fd, off_t offset, size_t length);

 private:
  struct DropTask {
    DropTask(int fd, off_t offset, size_t length)
        : fd(fd), offset(offset), length(length) {}

    int fd;
    off_t offset;
    size_t length;
  };

  static int DoDrop(void* meta, bthread::TaskIterator<DropTask>& iter);

  std::atomic<bool> running_;
  bthread::ExecutionQueueId<DropTask> drop_page_cache_queue_id_;
};

}  // namespace utils
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_UTILS_PAGE_CACHE_MANAGER_H_
