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

#ifndef DINGOFS_SRC_CLIENT_BLOCKCACHE_AIO_USRBIO_H_
#define DINGOFS_SRC_CLIENT_BLOCKCACHE_AIO_USRBIO_H_

#include <bthread/mutex.h>
#include <butil/iobuf.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/uio.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <unordered_map>

#include "client/blockcache/aio.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace client {
namespace blockcache {

#ifdef WITH_LIBUSRBIO

namespace internal {

#include <hf3fs_usrbio.h>

using utils::RWLock;

class Hf3fs {
 public:
  using Ior = struct hf3fs_ior;
  using Iov = struct hf3fs_iov;
  using Cqe = struct hf3fs_cqe;

  // Wrapper for 3fs usrbio interface
  static Status ExtractMountPoint(const std::string& path,
                                  std::string* mountpoint);
  static Status IorCreate(Ior* ior, const std::string& mountpoint,
                          uint32_t iodepth, bool for_read);
  static void IorDestroy(Ior* ior);
  static Status IovCreate(Iov* iov, const std::string& mountpoint,
                          size_t total_size);
  static void IovDestory(Iov* iov);
  static Status RegFd(int fd);
  static void DeRegFd(int fd);
  static Status PrepIo(Ior* ior, Iov* iov, bool for_read, void* buffer, int fd,
                       off_t offset, size_t length, void* userdata);
  static Status SubmitIos(Ior* ior);
  static int WaitForIos(Ior* ior, Cqe* cqes, uint32_t iodepth,
                        uint32_t timeout_ms);
};

// TODO: implement lock-free pool
class IovPool {
 public:
  IovPool(Hf3fs::Iov* iov);

  void Init(uint32_t blksize, uint32_t blocks);

  char* New();

  void Delete(const char* mem);

 private:
  char* mem_start_;
  uint32_t blksize_;
  std::queue<int> queue_;
  bthread::Mutex mutex_;
};

class Openfiles {
  using OpenFunc = std::function<Status(int fd)>;
  using CloseFunc = std::function<void(int fd)>;

 public:
  Openfiles() = default;

  Status Open(int fd, OpenFunc open_func);

  void Close(int fd, CloseFunc close_func);

 private:
  RWLock rwlock_;
  std::unordered_map<int, int> files_;  // mapping: fd -> refs
};

};  // namespace internal

// Usrbio(User Space Ring Based IO), not thread-safe
class Usrbio : public IoRing {
  using Hf3fs = internal::Hf3fs;
  using IovPool = internal::IovPool;
  using Openfiles = internal::Openfiles;

 public:
  explicit Usrbio(const std::string& mountpoint, uint32_t blksize);

  Status Init(uint32_t iodepth) override;

  void Shutdown() override;

  Status PrepareIo(Aio* aio) override;

  Status SubmitIo() override;

  Status WaitIo(uint32_t timeout_ms, std::vector<Aio*>* aios) override;

  void PostIo(Aio* aio) override;

 private:
  std::atomic<bool> running_;
  std::string mountpoint_;
  uint32_t iodepth_;
  uint32_t blksize_;
  Hf3fs::Ior ior_r_;
  Hf3fs::Ior ior_w_;
  Hf3fs::Iov iov_;
  Hf3fs::Cqe* cqes_;
  std::unique_ptr<IovPool> iov_pool_;
  std::unique_ptr<Openfiles> openfiles_;
};

#else

class Usrbio : public IoRing {
 public:
  Usrbio(const std::string& /*mountpoint*/, uint32_t /*blksize*/){};

  Status Init(uint32_t /*iodepth*/) override {
    return Status::NotSupport("not support 3fs usrbio");
  }

  void Shutdown() override {}

  Status PrepareIo(Aio* /*aio*/) override {
    return Status::NotSupport("not support 3fs usrbio");
  }

  Status SubmitIo() override {
    return Status::NotSupport("not support 3fs usrbio");
  }

  Status WaitIo(uint32_t /*timeout_ms*/, std::vector<Aio*>* /*aios*/) override {
    return Status::NotSupport("not support 3fs usrbio");
  }

  void PostIo(Aio* aio) override {}
};

#endif

}  // namespace blockcache
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_BLOCKCACHE_AIO_USRBIO_H_
