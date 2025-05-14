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
 * Created Date: 2025-05-15
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_STORAGE_AIO_USRBIO_HELPER_H_
#define DINGOFS_SRC_CACHE_STORAGE_AIO_USRBIO_HELPER_H_

#include <queue>

#include "cache/common/types.h"
#include "cache/storage/aio/usrbio_api.h"

namespace dingofs {
namespace cache {
namespace storage {

// TODO: implement lock-free pool
class IOVBuffer {
 public:
  explicit IOVBuffer(hf3fs::iov* iov);

  void Init(uint32_t blksize, uint32_t blocks);

  char* GetBlock();

  void ReleaseBlock(const char* mem);

 private:
  char* mem_start_;
  uint32_t blksize_;
  std::queue<int> queue_;
  bthread::Mutex mutex_;
};

class Openfiles {
 public:
  using OpenFunc = std::function<Status(int fd)>;
  using CloseFunc = std::function<void(int fd)>;

  Openfiles() = default;

  // open_func will invoked for file first opened
  Status Open(int fd, OpenFunc open_func);

  // close_func will invoked for file's refs decreased to 0
  void Close(int fd, CloseFunc close_func);

 private:
  BthreadRWLock rwlock_;
  std::unordered_map<int, int> files_;  // mapping: fd -> refs
};

}  // namespace storage
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_STORAGE_AIO_USRBIO_HELPER_H_
