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
 * Created Date: 2025-04-09
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_UTILS_AIO_URING_H_
#define DINGOFS_SRC_CACHE_UTILS_AIO_URING_H_

#include <butil/iobuf.h>
#include <liburing.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/uio.h>

#include <cstdint>

#include "cache/utils/aio.h"

namespace dingofs {
namespace cache {
namespace utils {

class LinuxIoUring : public IoRing {
 public:
  LinuxIoUring();

  Status Init(uint32_t io_depth) override;

  void Shutdown() override;

  Status PrepareIo(Aio* aio) override;

  Status SubmitIo() override;

  Status WaitIo(uint32_t timeout_ms, std::vector<Aio*>* aios) override;

  void PostIo(Aio* aio) override;

 private:
  static bool Supported();

 private:
  std::atomic<bool> running_;
  uint32_t io_depth_;
  struct io_uring io_uring_;
  int epoll_fd_;
};

}  // namespace utils
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_UTILS_AIO_URING_H_
