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

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_IO_URING_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_IO_URING_H_

#include <liburing.h>
#include <sys/epoll.h>

#include "cache/blockcache/aio.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

class IOUring {
 public:
  IOUring(const std::vector<iovec>& fixed_write_buffers,
          const std::vector<iovec>& fixed_read_buffers);
  Status Start();
  Status Shutdown();

  Status PrepareIO(Aio* aio);
  Status SubmitIO();
  int WaitIO(uint64_t timeout_ms, Aio* completed_aios[]);

 private:
  static bool Supported();

  void PrepWrite(io_uring_sqe* sqe, Aio* aio) const;
  void PrepRead(io_uring_sqe* sqe, Aio* aio) const;
  void OnComplete(Aio* aio, int result);

  std::atomic<bool> running_;
  io_uring io_uring_;
  off_t write_buf_index_offset_;
  off_t read_buf_index_offset_;
  std::vector<iovec> fixed_buffers_;
  int epoll_fd_;
};

using IOUringUPtr = std::unique_ptr<IOUring>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_IO_URING_H_
