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
 * Created Date: 2025-05-22
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_LOCAL_FILESYSTEM_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_LOCAL_FILESYSTEM_H_

#include <sys/types.h>

#include <cstddef>
#include <string>

#include "cache/blockcache/aio_queue.h"
#include "cache/blockcache/disk_cache_layout.h"
#include "cache/blockcache/disk_health_checker.h"
#include "cache/iutil/buffer_pool.h"
#include "cache/iutil/inflight_tracker.h"
#include "common/io_buffer.h"

namespace dingofs {
namespace cache {

class LocalFileSystem {
 public:
  explicit LocalFileSystem(DiskCacheLayoutSPtr layout);
  Status Start();
  Status Shutdown();

  Status WriteFile(const std::string& path, const IOBuffer* buffer);
  Status ReadFile(const std::string& path, off_t offset, size_t length,
                  IOBuffer* buffer);

 private:
  Status AioWrite(int fd, char* buffer, size_t length, int buf_index);
  Status AioRead(int fd, off_t offset, size_t length, char* buffer,
                 int buf_index);

  bool IsAligned(uint64_t n, uint64_t m) { return (n % m) == 0; }
  off_t AlignOffset(off_t offset);
  size_t AlignLength(size_t length);
  // Appends an O_DIRECT-aligned buffer to `out` for aio. With --fix_buffer the
  // buffer comes from a global slab pool — the recv pool for writes, the send
  // pool for reads — so it is at once the io_uring fixed buffer and the
  // RDMA-registered buffer (its meta carries the RDMA lkey). *buf_index is the
  // absolute io_uring fixed-buffer index (recv buffers are registered first,
  // send buffers after, offset by read_buf_offset_). Without --fix_buffer it
  // falls back to an aligned malloc and *buf_index is -1.
  Status AllocFixedBuffer(IOBuffer* out, size_t aligned_length, bool for_read,
                          int* buf_index);

  static constexpr size_t kAlignedIOBlockSize = 4096;

  std::atomic<bool> running_;
  DiskCacheLayoutSPtr layout_;
  iutil::InflightTracker inflight_;
  // io_uring registers recv-pool (write) buffers first then send-pool (read)
  // buffers; a read buffer's absolute fixed index is read_buf_offset_ + index.
  size_t read_buf_offset_;
  AioQueueUPtr aio_queue_;
  DiskHealthCheckerUPtr health_checker_;
};

using LocalFileSystemUPtr = std::unique_ptr<LocalFileSystem>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_LOCAL_FILESYSTEM_H_
