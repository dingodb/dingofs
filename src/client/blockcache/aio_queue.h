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

#ifndef DINGOFS_SRC_CLIENT_BLOCKCACHE_AIO_QUEUE_H_
#define DINGOFS_SRC_CLIENT_BLOCKCACHE_AIO_QUEUE_H_

#include <bthread/condition_variable.h>
#include <bthread/execution_queue.h>
#include <bthread/mutex.h>
#include <glog/logging.h>
#include <google/protobuf/stubs/callback.h>
#include <hf3fs_usrbio.h>
#include <sys/types.h>

#include <cstdint>
#include <memory>
#include <thread>

#include "client/blockcache/aio.h"
#include "client/blockcache/phase_timer.h"

namespace dingofs {
namespace client {
namespace blockcache {

/*
class ThrottleQueue {
 public:
  explicit ThrottleQueue(uint32_t capacity);

  void PushOne();

  void PopOne();

 private:
  uint32_t size_;
  uint32_t capacity_;
  bthread::Mutex mutex_;
  bthread::ConditionVariable cv_;
};
*/

class AioQueueImpl : public AioQueue {
  struct BthreadArg {
    BthreadArg(AioQueueImpl* queue, Aio* aio) : queue(queue), aio(aio) {}

    AioQueueImpl* queue;
    Aio* aio;
    Status status;
  };

 public:
  explicit AioQueueImpl(const std::shared_ptr<IoRing>& io_ring);

  bool Init(uint32_t iodepth) override;

  bool Shutdown() override;

  void Submit(Aio* aio) override;

 private:
  static void EnterPhase(Aio* aio, Phase phase);
  static void BatchEnterPhase(Aio* aios[], int n, Phase phase);

  void CheckIo(Aio* aio);
  static int PrepareIo(void* meta, bthread::TaskIterator<Aio*>& iter);
  void BatchSubmitIo(Aio* aios[], int n);
  void BackgroundWait();
  void RunClosureInBthread(Aio* aio);
  static void* RunClosure(void* arg);
  static Status GetStatus(Aio* aio);

 private:
  std::atomic<bool> running_;
  std::shared_ptr<IoRing> ioring_;
  bthread::ExecutionQueueId<Aio*> prep_io_queue_id_;
  std::thread bg_thread_;
  static constexpr uint32_t kSubmitBatchSize{16};
};

}  // namespace blockcache
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_BLOCKCACHE_AIO_QUEUE_H_
