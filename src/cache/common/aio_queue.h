/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2025-02-19
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_COMMON_AIO_QUEUE_H_
#define DINGOFS_SRC_CACHE_COMMON_AIO_QUEUE_H_

#include <bthread/execution_queue.h>
#include <liburing.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/uio.h>

#include <functional>
#include <thread>
#include <vector>

namespace dingofs {
namespace cache {
namespace common {

enum class AioType : uint8_t {
  kWrite,
  kRead,
};

struct AioContext {
  using AioCallback = std::function<void(AioContext* aio, int rc)>;

  AioContext() = default;

  AioContext(AioType type, int fd, off_t offset, size_t length, char* buffer,
             AioCallback aio_cb)
      : type(type),
        fd(fd),
        offset(offset),
        length(length),
        buffer(buffer),
        aio_cb(aio_cb) {}

  AioType type;
  int fd;
  off_t offset;
  size_t length;
  char* buffer;
  AioCallback aio_cb;
};

class AioQueue {
 public:
  virtual ~AioQueue() = default;

  // static bool Supported();

  virtual bool Init(uint32_t io_depth) = 0;

  virtual bool Shutdown() = 0;

  virtual void Submit(const std::vector<AioContext*>& iovec) = 0;
};

class AioQueueImpl : AioQueue {
 public:
  AioQueueImpl();

  bool Init(uint32_t io_depth) override;

  bool Shutdown() override;

  void Submit(const std::vector<AioContext*>& iovec) override;

 private:
  static int BatchSubmit(void* meta,
                         bthread::TaskIterator<std::vector<AioContext*>>& iter);

  void PrepareSqe(const AioContext* aio);

  void AioWait();

  int GetCompleted(int batch_size, std::vector<AioContext*>* iovec,
                   std::vector<int>* rcv);

  static void RunCallback(const std::vector<AioContext*>& iovec,
                          const std::vector<int>& rcv);

 private:
  std::atomic<bool> running_;
  struct io_uring io_uring_;
  int epoll_fd_;
  std::thread thread_;  // wait aio completed
  bthread::ExecutionQueueId<std::vector<AioContext*>> submit_queue_id_;
};

}  // namespace common
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_COMMON_AIO_QUEUE_H_
