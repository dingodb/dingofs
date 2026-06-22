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

#ifndef DINGOFS_SRC_CACHE_LOCAL_LOCAL_FILESYSTEM_H_
#define DINGOFS_SRC_CACHE_LOCAL_LOCAL_FILESYSTEM_H_

#include <sys/types.h>

#include <cstddef>
#include <memory>
#include <string>

#include "cache/infiniband/common.h"
#include "cache/infiniband/memory.h"
#include "cache/iutil/buffer_pool.h"
#include "cache/iutil/inflight_tracker.h"
#include "cache/local/aio_queue.h"
#include "cache/local/disk_cache_layout.h"
#include "cache/local/disk_health_checker.h"
#include "common/io_buffer.h"

namespace dingofs {
namespace cache {

class FixedBuffers {
 public:
  FixedBuffers();

  Status Alloc(size_t size, bool for_read, IOBuffer* buffer, int* buf_index);
  bool IsFixed(const IOBuffer* buffer, bool for_read, int* buf_index);
  std::vector<iovec> Fetch();

 private:
  int GetIndex(infiniband::RDMABuffer* rdma_buffer, bool for_read);

  infiniband::RDMABufferPool* write_pool_;
  infiniband::RDMABufferPool* read_pool_;
};

using FixedBuffersUPtr = std::unique_ptr<FixedBuffers>;

class LocalFileSystem {
 public:
  explicit LocalFileSystem(DiskCacheLayoutSPtr layout);
  Status Start();
  Status Shutdown();

  Status WriteFile(const std::string& path, const IOBuffer* buffer);
  Status ReadFile(const std::string& path, off_t offset, size_t length,
                  IOBuffer* buffer);

 private:
  static constexpr size_t kAlignedIOBlockSize = 4096;

  Status AioWrite(int fd, char* buffer, size_t length, int buf_index);
  Status AioRead(int fd, off_t offset, size_t length, char* buffer,
                 int buf_index);

  bool IsAligned(uint64_t n, uint64_t m);
  bool IsAligned(IOBuffer* buffer);
  off_t AlignOffset(off_t offset);
  size_t AlignLength(size_t length);

  std::atomic<bool> running_;
  DiskCacheLayoutSPtr layout_;
  FixedBuffersUPtr fixed_buffers_;
  iutil::InflightTracker inflight_;
  AioQueueUPtr aio_queue_;
  DiskHealthCheckerUPtr health_checker_;
};

using LocalFileSystemUPtr = std::unique_ptr<LocalFileSystem>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_LOCAL_LOCAL_FILESYSTEM_H_
