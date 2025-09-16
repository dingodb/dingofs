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

#include <bthread/execution_queue.h>

#include <memory>

#include "cache/storage/aio/aio.h"
#include "cache/utils/infight_throttle.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

class AioQueueImpl : public AioQueue {
 public:
  explicit AioQueueImpl(std::shared_ptr<IORing> io_ring);

  Status Start() override;
  Status Shutdown() override;

  void Submit(Aio* aio) override;

 private:
  static constexpr uint32_t kSubmitBatchSize = 16;

  // aio steps
  void CheckIO(Aio* aio);
  static int PrepareIO(void* meta, bthread::TaskIterator<Aio*>& iter);
  void BatchSubmitIO(const std::vector<Aio*>& aios);
  void BackgroundWait();

  // handle status
  void OnError(Aio* aio, Status status);
  void OnCompleted(Aio* aio);
  void RunClosure(Aio* aio);

  // utility
  static std::string LastStep(Aio* aio);
  static void NextStep(Aio* aio, const std::string& step_name);
  static void BatchNextStep(const std::vector<Aio*>& aios,
                            const std::string& step_name);
  static uint64_t GetTotalLength(const std::vector<Aio*>& aios);

  std::atomic<bool> running_;
  InflightThrottleUPtr infights_;
  std::shared_ptr<IORing> ioring_;
  bthread::ExecutionQueueId<Aio*> prep_io_queue_id_;  // for prepare io
  std::thread bg_wait_thread_;                        // for wait io
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_STORAGE_AIO_AIO_QUEUE_H_
