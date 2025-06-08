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

#ifndef DINGOFS_SRC_CACHE_STORAGE_HF3FS_H_
#define DINGOFS_SRC_CACHE_STORAGE_HF3FS_H_

#include <sys/types.h>

#include <cstddef>
#include <functional>
#include <string>

#include "cache/storage/aio/aio.h"
#include "cache/storage/filesystem_base.h"
#include "cache/storage/page_cache_manager.h"

namespace dingofs {
namespace cache {

class HF3FS final : public FileSystemBase {
 public:
  HF3FS(const std::string& mountpoint, CheckStatusFunc check_status_func);

  Status Init() override;
  Status Destroy() override;

  Status WriteFile(const std::string& path, const IOBuffer& buffer,
                   WriteOption option) override;
  Status ReadFile(const std::string& path, off_t offset, size_t length,
                  IOBuffer* buffer, ReadOption option) override;

 private:
  Status AioWrite(int fd, const IOBuffer& buffer);
  Status AioRead(int fd, off_t offset, size_t length, IOBuffer* buffer);

  std::atomic<bool> running_;
  IORingSPtr io_ring_w_;
  IORingSPtr io_ring_r_;
  AioQueueUPtr aio_queue_w_;
  AioQueueUPtr aio_queue_r_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_STORAGE_HF3FS_H_
