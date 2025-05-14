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

#ifndef DINGOFS_SRC_CACHE_STORAGE_AIO_AIO_QUEUE_H_
#define DINGOFS_SRC_CACHE_STORAGE_AIO_AIO_QUEUE_H_

#include <bthread/condition_variable.h>
#include <bthread/execution_queue.h>
#include <bthread/mutex.h>
#include <glog/logging.h>
#include <google/protobuf/stubs/callback.h>
#include <sys/types.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>

#include "base/math/math.h"
#include "cache/storage/aio/aio.h"
#include "cache/utils/helper.h"
#include "cache/utils/phase_timer.h"

namespace dingofs {
namespace cache {
namespace storage {

class ThrottleQueue {
 public:
  ThrottleQueue(uint32_t capacity) : size_(0), capacity_(capacity) {}

  void AddOne() {
    std::unique_lock<bthread::Mutex> lk(mutex_);
    while (size_ == capacity_) {
      cv_.wait(lk);
    }
    size_++;
  }

  void SubOne() {
    std::unique_lock<bthread::Mutex> lk(mutex_);
    CHECK(size_ > 0);
    size_--;
    cv_.notify_one();
  }

 private:
  uint32_t size_;
  uint32_t capacity_;
  bthread::Mutex mutex_;
  bthread::ConditionVariable cv_;
};

class AioQueueImpl : public AioQueue {
 public:
  explicit AioQueueImpl(std::shared_ptr<IORing> io_ring);

  Status Init() override;
  Status Shutdown() override;

  void Submit(AioClosure* aio) override;

 private:
  static void EnterPhase(AioClosure* aio, utils::Phase phase);
  static void BatchEnterPhase(AioClosure* aios[], int n, utils::Phase phase);

  void CheckIO(AioClosure* aio);
  static int PrepareIO(void* meta, bthread::TaskIterator<AioClosure*>& iter);
  void BatchSubmitIO(AioClosure* aios[], int n);
  void BackgroundWait();

  void RunClosure(AioClosure* aio);
  static Status GetStatus(AioClosure* aio);

  std::atomic<bool> running_;
  std::shared_ptr<IORing> ioring_;
  bthread::ExecutionQueueId<AioClosure*> prep_io_queue_id_;
  std::thread bg_wait_thread_;
  ThrottleQueue queued_;
  static constexpr uint32_t kSubmitBatchSize{16};
};

}  // namespace storage
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_STORAGE_AIO_AIO_QUEUE_H_
