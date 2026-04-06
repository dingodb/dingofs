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

#include <gtest/gtest_prod.h>
#include <liburing.h>
#include <sys/epoll.h>

#include <memory>
#include <ostream>
#include <vector>

#include "cache/blockcache/aio.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

struct IOUringOptions {
  uint32_t entries{4096};
  bool use_sqpoll{true};
  std::vector<iovec> fixed_write_buffers;
  std::vector<iovec> fixed_read_buffers;
};

class IOUring {
 public:
  explicit IOUring(IOUringOptions options = {});
  ~IOUring();

  Status Start();
  Status Shutdown();

  Status PrepareIO(Aio* aio);
  Status SubmitIO();
  int WaitIO(uint64_t timeout_ms, Aio* completed_aios[]);

 private:
  FRIEND_TEST(IOUringTest, FixedBuffers);
  FRIEND_TEST(IOUringTest, OnComplete);
  friend std::ostream& operator<<(std::ostream& os, const IOUring& r);

  struct FixedBuffers {
    FixedBuffers(std::vector<iovec> write_buffers,
                 std::vector<iovec> read_buffers)
        : write_count(write_buffers.size()) {
      buffers.reserve(write_buffers.size() + read_buffers.size());
      buffers.insert(buffers.end(), write_buffers.begin(), write_buffers.end());
      buffers.insert(buffers.end(), read_buffers.begin(), read_buffers.end());
    }

    int GetIndex(bool for_read, int buf_index) const {
      size_t offset = for_read ? write_count : 0;
      size_t count = for_read ? buffers.size() - write_count : write_count;
      if (buf_index < 0 || static_cast<size_t>(buf_index) >= count) {
        return -1;
      }
      return static_cast<int>(offset) + buf_index;
    }

    const size_t write_count{0};
    std::vector<iovec> buffers;
  };

  static bool Supported();
  Status InitIOUring();
  Status RegisterBuffers();
  Status SetupEpoll();
  void Cleanup();

  void PrepWrite(io_uring_sqe* sqe, Aio* aio) const;
  void PrepRead(io_uring_sqe* sqe, Aio* aio) const;
  void OnComplete(Aio* aio, int result);

  IOUringOptions options_;
  std::atomic<bool> running_;
  io_uring io_uring_;
  std::unique_ptr<FixedBuffers> fixed_buffers_;
  int epoll_fd_;
};

using IOUringUPtr = std::unique_ptr<IOUring>;

std::ostream& operator<<(std::ostream& os, const IOUring& r);

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_IO_URING_H_
