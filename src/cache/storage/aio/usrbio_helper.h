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

#ifdef WITH_LIBUSRBIO

#ifndef DINGOFS_SRC_CACHE_STORAGE_AIO_USRBIO_HELPER_H_
#define DINGOFS_SRC_CACHE_STORAGE_AIO_USRBIO_HELPER_H_

#include <queue>

#include "cache/storage/aio/usrbio_api.h"

namespace dingofs {
namespace cache {

class BlockBufferPool {
 public:
  BlockBufferPool(USRBIOApi::IOV* iov, uint32_t blksize, uint32_t blocks);

  char* Get();
  void Release(const char* mem);

 private:
  char* mem_start_;
  uint32_t blksize_;
  std::queue<uint32_t> free_list_;  // TODO(Wine93): implement lock-free pool
  BthreadMutex mutex_;
};

// Cache registed fd
class FdRegister {
 public:
  FdRegister() = default;

  Status RegFd(int fd);
  void DeRegFd(int fd);

 private:
  BthreadMutex mutex_;
  std::unordered_map<int, int> registed_fds_;  // mapping: fd -> refs
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_STORAGE_AIO_USRBIO_HELPER_H_

#endif  // WITH_LIBUSRBIO
