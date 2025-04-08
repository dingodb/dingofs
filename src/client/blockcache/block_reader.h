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
 * Created Date: 2025-03-30
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CLIENT_BLOCKCACHE_BLOCK_READER_H_
#define DINGOFS_SRC_CLIENT_BLOCKCACHE_BLOCK_READER_H_

#include <cstddef>
#include <memory>

#include "client/blockcache/aio_queue.h"
#include "client/blockcache/local_filesystem.h"

namespace dingofs {
namespace client {
namespace blockcache {

class BlockReader {
 public:
  virtual ~BlockReader() = default;

  virtual Status ReadAt(off_t offset, size_t length, char* buffer) = 0;

  virtual void Close() = 0;
};

class LocalBlockReader : public BlockReader {
 public:
  LocalBlockReader(int fd, std::shared_ptr<LocalFileSystem> fs);

  ~LocalBlockReader() override = default;

  Status ReadAt(off_t offset, size_t length, char* buffer) override;

  void Close() override;

 private:
  int fd_;
  std::shared_ptr<LocalFileSystem> fs_;
};

class RemoteBlockReader : public BlockReader {
 public:
  RemoteBlockReader(int fd, size_t blksize, std::shared_ptr<LocalFileSystem> fs,
                    std::shared_ptr<AioQueue> aio_queue);

  ~RemoteBlockReader() override = default;

  Status ReadAt(off_t offset, size_t length, char* buffer) override;

  void Close() override;

 private:
  int fd_;
  size_t blksize_;
  std::shared_ptr<LocalFileSystem> fs_;
  std::shared_ptr<AioQueue> aio_queue_;
};

}  // namespace blockcache
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_BLOCKCACHE_BLOCK_READER_H_
