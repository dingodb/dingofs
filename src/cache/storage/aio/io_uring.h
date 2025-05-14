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

#ifndef DINGOFS_SRC_CACHE_STORAGE_AIO_IO_URING_H_
#define DINGOFS_SRC_CACHE_STORAGE_AIO_IO_URING_H_

#include <butil/iobuf.h>
#include <liburing.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/uio.h>

#include <cstdint>

#include "cache/storage/aio/aio.h"

namespace dingofs {
namespace cache {
namespace storage {

class LinuxIOUring : public IORing {
 public:
  LinuxIOUring(uint32_t io_depth);

  uint32_t GetIODepth() override;

  Status Init() override;
  Status Shutdown() override;

  Status PrepareIO(AioClosure* aio) override;
  Status SubmitIO() override;
  Status WaitIO(uint32_t timeout_ms, Aios* aios) override;

 private:
  static bool Supported();

  void PrepWrite(io_uring_sqe* sqe, AioClosure* aio);
  void PrepRead(io_uring_sqe* sqe, AioClosure* aio);
  void OnCompleted(AioClosure* aio, int retcode);

  std::atomic<bool> running_;
  uint32_t io_depth_;
  struct io_uring io_uring_;
  int epoll_fd_;
};

}  // namespace storage
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_STORAGE_AIO_IO_URING_H_
