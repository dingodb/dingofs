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

#include "cache/blockcache/page_cache_manager.h"

#include "cache/common/common.h"
#include "cache/utils/posix.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace blockcache {

using dingofs::cache::utils::Posix;

PageCacheManager::PageCacheManager() : running_(false) {}

bool PageCacheManager::Start() {
  if (!running_.exchange(true)) {
    return true;
    bthread::ExecutionQueueOptions queue_options;
    queue_options.use_pthread = true;
    int rc = bthread::execution_queue_start(&drop_page_cache_queue_id_,
                                            &queue_options, DoDrop, this);
    if (rc != 0) {
      LOG(ERROR) << "execution_queue_start() failed, rc=" << rc;
    }
    return rc == 0;
  }
  return true;
}

bool PageCacheManager::Stop() {
  if (running_.exchange(false)) {
    bthread::execution_queue_stop(drop_page_cache_queue_id_);
    return bthread::execution_queue_join(drop_page_cache_queue_id_) == 0;
  }
  return true;
}

void PageCacheManager::DropPageCache(int fd, uint64_t offset, uint64_t length) {
  DropTask task(fd, offset, length);
  CHECK_EQ(0,
           bthread::execution_queue_execute(drop_page_cache_queue_id_, task));
}

int PageCacheManager::DoDrop(void* /*meta*/,
                             bthread::TaskIterator<DropTask>& iter) {
  for (; iter; iter++) {
    auto& task = *iter;
    auto status = Posix::PosixFAdvise(task.fd, task.offset, task.length,
                                      POSIX_FADV_DONTNEED);
    if (!status.ok()) {
      LOG(WARNING) << "Drop page cache failed: fd = " << task.fd
                   << ", status =" << status.ToString();
    }
  }
  return 0;
}

}  // namespace blockcache
}  // namespace cache
}  // namespace dingofs
