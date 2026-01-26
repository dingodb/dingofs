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
#include "cache/common/context.h"
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

  Status WriteFile(ContextSPtr ctx, const std::string& path,
                   const IOBuffer* buffer);
  Status ReadFile(ContextSPtr ctx, const std::string& path, off_t offset,
                  size_t length, IOBuffer* buffer);

 private:
  Status AioWrite(ContextSPtr ctx, int fd, char* buffer, size_t length,
                  int buf_index);
  Status AioRead(ContextSPtr ctx, int fd, off_t offset, size_t length,
                 char* buffer, int buf_index);

  bool IsAligned(uint64_t n, uint64_t m) { return (n % m) == 0; }
  off_t AlignOffset(off_t offset);
  size_t AlignLength(size_t length);
  int AllocateAlignedMemory(IOBuffer* buffer, size_t aligned_length,
                            bool for_read);

  static constexpr size_t kAlignedIOBlockSize = 4096;

  std::atomic<bool> running_;
  DiskCacheLayoutSPtr layout_;
  BufferPoolUPtr write_buffer_pool_;
  BufferPoolUPtr read_buffer_pool_;
  iutil::InflightTracker inflight_;
  AioQueueUPtr aio_queue_;
  DiskHealthCheckerUPtr health_checker_;
};

using LocalFileSystemUPtr = std::unique_ptr<LocalFileSystem>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_LOCAL_FILESYSTEM_H_
