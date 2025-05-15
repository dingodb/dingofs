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

#ifndef DINGOFS_SRC_CACHE_STORAGE_AIO_USRBIO_H_
#define DINGOFS_SRC_CACHE_STORAGE_AIO_USRBIO_H_

#include <bthread/mutex.h>
#include <butil/iobuf.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/uio.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <unordered_map>

#include "cache/storage/aio/aio.h"
#include "cache/storage/aio/usrbio_api.h"
#include "cache/storage/aio/usrbio_helper.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace cache {
namespace storage {

// USRBIO(User Space Ring Based IO), not thread-safe
//
// memory management:
//                mmap                  without-copying
//   DMA buffer -------- Shared memory ----------------- IOBuffer
//
// TODO(Wine93): please test peformance for memory-copying and without-copying
// TODO(Wine93): please check whether the DMA buffer is mmap with shared memory
class USRBIO : public IORing {
 public:
  USRBIO(const std::string& mountpoint, uint32_t blksize, uint32_t io_depth,
         bool for_read);

  uint32_t GetIODepth() override;

  Status Init() override;
  Status Shutdown() override;

  Status PrepareIO(AioClosure* aio) override;
  Status SubmitIO() override;
  Status WaitIO(uint32_t timeout_ms, Aios* aios) override;

 private:
  void OnCompleted(AioClosure* aio, int retcode);

  std::atomic<bool> running_;
  std::string mountpoint_;
  uint32_t blksize_;
  uint32_t io_depth_;
  bool for_read_;
  hf3fs::ior ior_;
  hf3fs::iov iov_;
  hf3fs::cqe* cqes_;
  std::unique_ptr<IOVBuffer> iov_buffer_;
  std::unique_ptr<Openfiles> openfiles_;
};

// class USRBIO : public IORing {
//  public:
//   Status Init() override {
//     return Status::NotSupport("not support 3fs usrbio");
//   }
//
//   Status Shutdown() override {}
//
//   Status PrepareIO(AioClosure* /*aio*/) override {
//   return Status::NotSupport("not support 3fs usrbio");
// }
//
// Status SubmitIO() override {
//   return Status::NotSupport("not support 3fs usrbio");
// }
//
// Status WaitIO(uint32_t /*timeout_ms*/, std::vector<Aio*>* /*aios*/) override
// {
//   return Status::NotSupport("not support 3fs usrbio");
// }
//
// void PostIO(Aio* aio) override {}
// };

}  // namespace storage
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_STORAGE_AIO_USRBIO_H_
