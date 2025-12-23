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

#ifndef DINGOFS_SRC_CACHE_STORAGE_LOCAL_FILESYSTEM_H_
#define DINGOFS_SRC_CACHE_STORAGE_LOCAL_FILESYSTEM_H_

#include <bthread/mutex.h>
#include <butil/containers/flat_map.h>
#include <sys/types.h>

#include <cstddef>
#include <string>

#include "cache/storage/aio/aio.h"
#include "cache/storage/base_filesystem.h"
#include "cache/storage/filesystem.h"
#include "cache/storage/page_cache_manager.h"
#include "cache/utils/buffer_pool.h"

namespace dingofs {
namespace cache {

class Openfile {
 public:
  //

 private:
  butil::FlatMap<std::string, int> openfiles_;
};

class LocalFileSystem final : public BaseFileSystem {
 public:
  explicit LocalFileSystem(CheckStatusFunc check_status_func);

  Status Start() override;
  Status Shutdown() override;

  Status WriteFile(ContextSPtr ctx, const std::string& path,
                   const IOBuffer& buffer, WriteOption option) override;
  Status ReadFile(ContextSPtr ctx, const std::string& path, off_t offset,
                  size_t length, IOBuffer* buffer, ReadOption option) override;

 private:
  off_t AlignOffset(off_t offset);
  size_t AlignLength(size_t length);
  std::pair<off_t, size_t> AlignRequest(off_t offset, size_t length);

  Status AioWrite(ContextSPtr ctx, int fd, IOBuffer* buffer,
                  WriteOption option);
  Status AioRead(ContextSPtr ctx, int fd, off_t offset, size_t length,
                 IOBuffer* buffer, ReadOption option);
  Status MapFile(ContextSPtr ctx, int fd, off_t offset, size_t length,
                 IOBuffer* buffer, ReadOption option);

  void CloseFd(ContextSPtr ctx, int fd);
  void Unlink(ContextSPtr ctx, const std::string& path);

  IOBuffer CopyBuffer(const IOBuffer& src);

  std::atomic<bool> running_;
  BufferPoolSPtr buffer_pool_;
  IORingSPtr io_ring_;
  AioQueueUPtr aio_queue_;
  PageCacheManagerUPtr page_cache_manager_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_STORAGE_LOCAL_FILESYSTEM_H_
