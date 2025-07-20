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

#include "cache/storage/aio/aio.h"
#include "common/status.h"
#ifdef WITH_LIBUSRBIO
#include "cache/storage/aio/usrbio_api.h"
#include "cache/storage/aio/usrbio_helper.h"
#endif  // WITH_LIBUSRBIO

namespace dingofs {
namespace cache {

#ifdef WITH_LIBUSRBIO

// USRBIO(User Space Ring Based IO), not thread-safe
//
// memory management:
//                mmap                  without-copying
//   DMA buffer -------- Shared memory ----------------- IOBuffer
//
// TODO(Wine93): please test peformance for memory-copying and without-copying
// TODO(Wine93): please check whether the DMA buffer can mmapped with shared
// memory

class USRBIO final : public IORing {
 public:
  USRBIO(const std::string& mountpoint, uint32_t blksize, uint32_t iodepth,
         bool for_read);

  Status Start() override;
  Status Shutdown() override;

  Status PrepareIO(AioClosure* aio) override;
  Status SubmitIO() override;
  Status WaitIO(uint64_t timeout_ms, std::vector<AioClosure*>* aios) override;

  uint32_t GetIODepth() const override;

 private:
  struct ContextSPtr {
    Context(char* buffer) : buffer(buffer) {}
    char* buffer;
  };

  void OnCompleted(AioClosure* aio, int retcode);
  void HandleBuffer(AioClosure* aio, char* buffer);

  std::atomic<bool> running_;
  std::string mountpoint_;
  uint32_t blksize_;
  uint32_t iodepth_;
  bool for_read_;
  USRBIOApi::IOR ior_;
  USRBIOApi::IOV iov_;
  USRBIOApi::Cqe* cqes_;
  std::unique_ptr<BlockBufferPool> block_buffer_pool_;
  std::unique_ptr<FdRegister> fd_register_;
};

#else

#define RETURN_NOT_SUPPORT() return Status::NotSupport("not support 3fs usrbio")

class USRBIO final : public IORing {
 public:
  USRBIO(const std::string& /*mountpoint*/, uint32_t /*blksize*/,
         uint32_t /*iodepth*/, bool /*for_read*/) {}

  Status Start() override { RETURN_NOT_SUPPORT(); }
  Status Shutdown() override { RETURN_NOT_SUPPORT(); }

  Status PrepareIO(Aio* /*aio*/) override { RETURN_NOT_SUPPORT(); }
  Status SubmitIO() override { RETURN_NOT_SUPPORT(); }
  Status WaitIO(uint64_t /*timeout_ms*/, std::vector<Aio*>* /*aios*/) override {
    RETURN_NOT_SUPPORT();
  }
};

#endif  // WITH_LIBUSRBIO

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_STORAGE_AIO_USRBIO_H_
