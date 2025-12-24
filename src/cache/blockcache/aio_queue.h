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
 * Created Date: 2025-03-30
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_AIO_QUEUE_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_AIO_QUEUE_H_

#include <bthread/execution_queue.h>

#include <thread>

#include "cache/blockcache/aio.h"
#include "cache/blockcache/io_uring.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

class AioQueue {
 public:
  AioQueue(const std::vector<iovec>& fixed_write_buffers,
           const std::vector<iovec>& fixed_read_buffers);
  Status Start();
  Status Shutdown();

  void Submit(Aio* aio);

 private:
  static constexpr int kSubmitBatchSize = 16;

  static int PrepareIO(void* meta, bthread::TaskIterator<Aio*>& iter);
  void BatchSubmitIO(Aio* aios[], int n);
  void BackgroundWait();

  void OnError(Aio* aio, Status status);
  void OnComplete(Aio* aio);
  void RunClosure(Aio* aio);

  std::atomic<bool> running_;
  IOUringUPtr io_uring_;
  bthread::ExecutionQueueId<Aio*> prep_io_queue_id_;  // for prepare io
  Aio* prepared_aios_[kSubmitBatchSize];
  std::thread bg_wait_thread_;  // for wait io
};

using AioQueueUPtr = std::unique_ptr<AioQueue>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_AIO_QUEUE_H_
