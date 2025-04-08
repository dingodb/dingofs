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

#include "client/blockcache/aio_usrbio.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_format.h>
#include <bthread/bthread.h>
#include <bthread/types.h>
#include <butil/time.h>
#include <glog/logging.h>
#include <hf3fs_usrbio.h>
#include <sys/epoll.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <queue>

#include "client/blockcache/aio.h"
#include "client/blockcache/helper.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace client {
namespace blockcache {

namespace internal {

using utils::ReadLockGuard;
using utils::WriteLockGuard;

bool Hf3fs::ExtractMountPoint(const std::string& path,
                              std::string* mountpoint) {
  char mp[4096];
  int n = hf3fs_extract_mount_point(mp, sizeof(mp), path.c_str());
  if (n <= 0) {
    return false;
  }

  // TODO: check filesystem type
  *mountpoint = std::string(mp, n);
  return true;
}

bool Hf3fs::IorCreate(Ior* ior, const std::string& mountpoint, uint32_t iodepth,
                      bool for_read) {
  int rc =
      hf3fs_iorcreate4(ior, mountpoint.c_str(), iodepth, for_read, 0, 0, -1, 0);
  if (rc != 0) {
    LOG(ERROR) << Helper::Errorf(-rc, "hf3fs_iorcreate4(%s,%s)", mountpoint,
                                 (for_read ? "read" : "write"));
    return false;
  }
  return true;
}

void Hf3fs::IorDestroy(Ior* ior) { hf3fs_iordestroy(ior); }

bool Hf3fs::IovCreate(Iov* iov, const std::string& mountpoint,
                      size_t total_size) {
  auto rc = hf3fs_iovcreate(iov, mountpoint.c_str(), total_size, 0, -1);
  if (rc != 0) {
    LOG(ERROR) << Helper::Errorf(-rc, "hf3fs_iovcreate(%s,%d)", mountpoint,
                                 total_size);
    return false;
  }
  return true;
}

void Hf3fs::IovDestory(Iov* iov) { hf3fs_iovdestroy(iov); }

bool Hf3fs::RegFd(int fd) {
  int rc = hf3fs_reg_fd(fd, 0);
  if (rc > 0) {
    LOG(ERROR) << Helper::Errorf(rc, "hf3fs_reg_fd(fd=%d)", fd);
    return false;
  }
  return true;
}

void Hf3fs::DeRegFd(int fd) { hf3fs_dereg_fd(fd); }

bool Hf3fs::PrepIo(Ior* ior, Iov* iov, bool for_read, void* buffer, int fd,
                   off_t offset, size_t length, void* userdata) {
  // ptr 必须是 iov 的地址吗？
  int rc =
      hf3fs_prep_io(ior, iov, for_read, buffer, fd, offset, length, userdata);
  if (rc < 0) {
    LOG(ERROR) << Helper::Errorf(
        -rc, "hf3fs_prep_io(fd=%d,offset=%d,length=%d)", fd, offset, length);
    return false;
  }
  return true;
}

bool Hf3fs::SubmitIos(Ior* ior) {
  int rc = hf3fs_submit_ios(ior);
  if (rc != 0) {
    LOG(ERROR) << Helper::Errorf(-rc, "hf3fs_submit_ios()");
    return false;
  }
  return true;
}

int Hf3fs::WaitForIos(Ior* ior, Cqe* cqes, uint32_t iodepth,
                      uint32_t timeout_ms) {
  timespec ts;
  ts.tv_sec = timeout_ms / 1000;
  ts.tv_nsec = (timeout_ms % 1000) * 1000000L;

  int n = hf3fs_wait_for_ios(ior, cqes, iodepth, 1, &ts);
  if (n < 0) {
    std::cout << Helper::Errorf(-n, "hf3fs_wait_for_ios(%d,%d,%d)", iodepth, 1,
                                timeout_ms);
  }
  return n;
}

IovPool::IovPool(Hf3fs::Iov* iov) : mem_start_((char*)iov->base){};

void IovPool::Init(uint32_t blksize, uint32_t blocks) {
  blksize_ = blksize;
  for (int i = 0; i < blocks; i++) {
    queue_.push(i);
  }
}

char* IovPool::New() {
  std::lock_guard<bthread::Mutex> mtx(mutex_);
  CHECK_GE(queue_.size(), 0);
  int blkindex = queue_.front();
  queue_.pop();
  return mem_start_ + (blksize_ * blkindex);
};

void IovPool::Delete(const char* mem) {
  std::lock_guard<bthread::Mutex> mtx(mutex_);
  int blkindex = (mem - mem_start_) / blksize_;
  queue_.push(blkindex);
};

bool Openfiles::Open(int fd, OpenFunc open_func) {
  WriteLockGuard lk(rwlock_);
  auto iter = files_.find(fd);
  if (iter == files_.end()) {
    files_.insert({fd, 1});
    return open_func(fd);
  }
  iter->second++;
  return true;
}

void Openfiles::Close(int fd, CloseFunc close_func) {
  WriteLockGuard lk(rwlock_);
  auto iter = files_.find(fd);
  if (iter != files_.end()) {
    CHECK_GE(iter->second, 0);
    if (--iter->second == 0) {
      files_.erase(iter);
      close_func(fd);
    }
  }
}

};  // namespace internal

Usrbio::Usrbio(const std::string& mountpoint, uint32_t blksize)
    : running_(false),
      mountpoint_(mountpoint),
      blksize_(blksize),
      ior_r_(),
      ior_w_(),
      iov_(),
      cqes_(nullptr),
      openfiles_(std::make_unique<Openfiles>()) {}

bool Usrbio::Init(uint32_t iodepth) {
  if (running_.exchange(true)) {  // already running
    return true;
  }

  std::string real_mountpoint;
  bool succ = Hf3fs::ExtractMountPoint(mountpoint_, &real_mountpoint);
  if (!succ) {
    LOG(ERROR) << "The cache_dir(" << mountpoint_
               << ") is not format with 3fs filesystem.";
    return false;
  }
  mountpoint_ = real_mountpoint;

  // read io ring
  succ = Hf3fs::IorCreate(&ior_r_, mountpoint_, iodepth, true);
  if (!succ) {
    LOG(ERROR) << "Create 3fs write io ring failed.";
    return false;
  }

  // write io ring
  succ = Hf3fs::IorCreate(&ior_w_, mountpoint_, iodepth, false);
  if (!succ) {
    LOG(ERROR) << "Create 3fs read io ring failed.";
    return false;
  }

  // shared memory
  succ = Hf3fs::IovCreate(&iov_, mountpoint_, iodepth * blksize_);
  if (!succ) {
    LOG(ERROR) << "Create 3fs shared memory failed.";
    return false;
  }

  iodepth_ = iodepth;
  cqes_ = new Hf3fs::Cqe[iodepth];
  iov_pool_ = std::make_unique<IovPool>(&iov_);
  iov_pool_->Init(blksize_, iodepth);
  return true;
}

void Usrbio::Shutdown() {
  if (!running_.exchange(false)) {
    return;
  }

  Hf3fs::IorDestroy(&ior_r_);
  Hf3fs::IorDestroy(&ior_w_);
  delete[] cqes_;
}

bool Usrbio::PrepareIo(Aio* aio) {
  bool for_read = (aio->aio_type == AioType::kRead);
  Hf3fs::Ior* ior = for_read ? &ior_r_ : &ior_w_;
  if (!openfiles_->Open(aio->fd, [&](int fd) { return Hf3fs::RegFd(fd); })) {
    return false;
  }

  char* iov_buffer = iov_pool_->New();
  bool succ = Hf3fs::PrepIo(ior, &iov_, for_read, iov_buffer, aio->fd,
                            aio->offset, aio->length, aio);
  if (!succ) {
    iov_pool_->Delete(iov_buffer);
    openfiles_->Close(aio->fd, [&](int fd) { Hf3fs::DeRegFd(aio->fd); });
    return false;
  }

  aio->iov_buffer = iov_buffer;
  return true;
}

bool Usrbio::SubmitIo() { return Hf3fs::SubmitIos(&ior_r_); }

bool Usrbio::WaitIo(uint32_t timeout_ms, std::vector<Aio*>* aios) {
  aios->clear();

  int n = Hf3fs::WaitForIos(&ior_r_, cqes_, iodepth_, timeout_ms);
  if (n < 0) {
    return false;
  }

  for (int i = 0; i < n; i++) {
    auto* aio = (Aio*)(cqes_[i].userdata);
    aio->retcode = (cqes_[i].result >= 0) ? 0 : -1;
    aios->emplace_back(aio);
  }
  return true;
}

void Usrbio::PostIo(Aio* aio) {
  if (aio->aio_type == AioType::kRead) {
    memcpy(aio->buffer, aio->iov_buffer, aio->length);
    iov_pool_->Delete(aio->iov_buffer);
  }
  openfiles_->Close(aio->fd, [&](int fd) { Hf3fs::DeRegFd(aio->fd); });
}

}  // namespace blockcache
}  // namespace client
}  // namespace dingofs
