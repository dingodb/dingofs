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
 * Created Date: 2024-11-05
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_BLOCKCACHE_AIO_H_
#define CURVEFS_SRC_CLIENT_BLOCKCACHE_AIO_H_

#include <bits/types/struct_iovec.h>
#include <liburing.h>
#include <sys/epoll.h>
#include <sys/uio.h>

#include <functional>
#include <mutex>
#include <ostream>
#include <string>
#include <thread>
#include <vector>

namespace curvefs {
namespace client {
namespace blockcache {

enum class AioType {
  kWrite,
  kRead,
};

struct AioContext {
  using AioCallback = std::function<void(AioContext*)>;

  AioContext(AioType type, int fd, off_t offset, size_t length, char* buffer,
             AioCallback aio_cb)
      : type(type),
        fd(fd),
        offset(offset),
        length(length),
        buffer(buffer),
        aio_cb(aio_cb),
        retval(-1) {}

  AioType type;
  int fd;
  off_t offset;
  size_t length;
  char* buffer;
  AioCallback aio_cb;
  // retval:
  int retval;
};

class AioQueue {
 public:
  AioQueue();

  static bool Supported();

  bool Init(uint32_t io_depth);

  void Shutdown();

  void Submit(std::vector<AioContext> aio_contexts);

 private:
  std::string SysErr();

  void Enqueue(AioContext* ctx);

  void ReapWorker();

  int GetCompleted(int max, std::vector<AioContext>* contexts);

  void OnComplete(std::vector<AioContext>& contexts);

 private:
  struct IOUring {
    struct io_uring ring;
    int epoll_fd;
    std::mutex sq_mutex;  // submit queue mutex
  };

 private:
  std::atomic<bool> running_;
  std::thread thread_id_;
  IOUring io_uring_;
};

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BLOCKCACHE_AIO_H_
