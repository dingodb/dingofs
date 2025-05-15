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
 * Created Date: 2025-05-13
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_STORAGE_FILESYSTEM_H_
#define DINGOFS_SRC_CACHE_STORAGE_FILESYSTEM_H_

#include <cstdint>
#include <memory>
#include <string>

#include "cache/common/common.h"
#include "cache/storage/aio/aio.h"
#include "cache/storage/aio/aio_queue.h"
#include "cache/storage/buffer.h"
#include "cache/utils/page_cache_manager.h"

namespace dingofs {
namespace cache {
namespace storage {

class FileSystem {
 public:
  virtual ~FileSystem() = default;

  virtual Status Init() = 0;
  virtual Status Shutdown() = 0;

  virtual Status Write(int fd, IOBuffer* buffer) = 0;
  virtual Status Read(int fd, off_t offset, size_t length,
                      IOBuffer* buffer) = 0;
};

// local filesyste, link xfs、ext4
class LocalFileSystem : public FileSystem {
 public:
  explicit LocalFileSystem(uint32_t io_depth);

  Status Init() override;
  Status Shutdown() override;

  Status Write(int fd, IOBuffer* buffer) override;
  Status Read(int fd, off_t offset, size_t length, IOBuffer* buffer) override;

 private:
  Status MapFile(int fd, off_t offset, size_t length, IOBuffer* buffer);

 private:
  uint32_t io_depth_;
  std::atomic<bool> running_;
  std::shared_ptr<IORing> io_ring_;
  std::unique_ptr<AioQueue> aio_queue_;
  std::unique_ptr<utils::PageCacheManager> page_cache_manager_;
};

// hf3fs which write/read by usrbio.
class HF3FS : public FileSystem {
 public:
  HF3FS(const std::string& mountpoint, uint32_t blksize, uint32_t io_depth);

  Status Init() override;
  Status Shutdown() override;

  Status Write(int fd, IOBuffer* buffer) override;
  Status Read(int fd, off_t offset, size_t length, IOBuffer* buffer) override;

 public:
  uint32_t io_depth_;
  std::atomic<bool> running_;
  std::shared_ptr<IORing> io_ring_w_;
  std::shared_ptr<IORing> io_ring_r_;
  std::unique_ptr<AioQueue> aio_queue_w_;
  std::unique_ptr<AioQueue> aio_queue_r_;
};

}  // namespace storage
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_STORAGE_FILESYSTEM_H_
