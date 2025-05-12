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

#include "cache/utils/aio_usrbio.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_format.h>
#include <bthread/bthread.h>
#include <bthread/types.h>
#include <butil/time.h>
#include <glog/logging.h>
#include <sys/epoll.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <queue>

#include "cache/utils/aio.h"
#include "cache/utils/helper.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace cache {
namespace utils {

#ifdef WITH_LIBUSRBIO

namespace internal {

using dingofs::utils::ReadLockGuard;
using dingofs::utils::WriteLockGuard;

Status Hf3fs::ExtractMountPoint(const std::string& path,
                                std::string* mountpoint) {
  char mp[4096];
  int n = hf3fs_extract_mount_point(mp, sizeof(mp), path.c_str());
  if (n <= 0) {
    return Status::Internal("hf3fs_extract_mount_point() failed");
  }

  // TODO: check filesystem type
  *mountpoint = std::string(mp, n);
  return Status::OK();
}

Status Hf3fs::IorCreate(Ior* ior, const std::string& mountpoint,
                        uint32_t iodepth, bool for_read) {
  int rc =
      hf3fs_iorcreate4(ior, mountpoint.c_str(), iodepth, for_read, 0, 0, -1, 0);
  if (rc != 0) {
    LOG(ERROR) << Errorf(-rc, "hf3fs_iorcreate4(%s,%s)", mountpoint,
                         (for_read ? "read" : "write"));
    return Status::Internal("hf3fs_iorcreate4() failed");
  }
  return Status::OK();
}

void Hf3fs::IorDestroy(Ior* ior) { hf3fs_iordestroy(ior); }

Status Hf3fs::IovCreate(Iov* iov, const std::string& mountpoint,
                        size_t total_size) {
  auto rc = hf3fs_iovcreate(iov, mountpoint.c_str(), total_size, 0, -1);
  if (rc != 0) {
    LOG(ERROR) << Errorf(-rc, "hf3fs_iovcreate(%s,%d)", mountpoint, total_size);
    return Status::Internal("hf3fs_iovcreate() failed");
  }
  return Status::OK();
}

void Hf3fs::IovDestory(Iov* iov) { hf3fs_iovdestroy(iov); }

Status Hf3fs::RegFd(int fd) {
  int rc = hf3fs_reg_fd(fd, 0);
  if (rc > 0) {
    LOG(ERROR) << Errorf(rc, "hf3fs_reg_fd(fd=%d)", fd);
    return Status::Internal("hf3fs_reg_fd() failed");
  }
  return Status::OK();
}

void Hf3fs::DeRegFd(int fd) { hf3fs_dereg_fd(fd); }

Status Hf3fs::PrepIo(Ior* ior, Iov* iov, bool for_read, void* buffer, int fd,
                     off_t offset, size_t length, void* userdata) {
  int rc =
      hf3fs_prep_io(ior, iov, for_read, buffer, fd, offset, length, userdata);
  if (rc < 0) {
    LOG(ERROR) << Errorf(-rc, "hf3fs_prep_io(fd=%d,offset=%d,length=%d)", fd,
                         offset, length);
    return Status::Internal("hf3fs_prep_io() failed");
  }
  return Status::OK();
}

Status Hf3fs::SubmitIos(Ior* ior) {
  int rc = hf3fs_submit_ios(ior);
  if (rc != 0) {
    LOG(ERROR) << Errorf(-rc, "hf3fs_submit_ios()");
    return Status::Internal("hf3fs_submit_ios() failed");
  }
  return Status::OK();
}

int Hf3fs::WaitForIos(Ior* ior, Cqe* cqes, uint32_t iodepth,
                      uint32_t timeout_ms) {
  timespec ts;
  ts.tv_sec = timeout_ms / 1000;
  ts.tv_nsec = (timeout_ms % 1000) * 1000000L;

  int n = hf3fs_wait_for_ios(ior, cqes, iodepth, 1, &ts);
  if (n < 0) {
    std::cout << Errorf(-n, "hf3fs_wait_for_ios(%d,%d,%d)", iodepth, 1,
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

Status Openfiles::Open(int fd, OpenFunc open_func) {
  WriteLockGuard lk(rwlock_);
  auto iter = files_.find(fd);
  if (iter == files_.end()) {
    files_.insert({fd, 1});
    return open_func(fd);
  }
  iter->second++;
  return Status::OK();
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

Status Usrbio::Init(uint32_t iodepth) {
  if (running_.exchange(true)) {  // already running
    return Status::OK();
  }

  std::string real_mountpoint;
  Status status = Hf3fs::ExtractMountPoint(mountpoint_, &real_mountpoint);
  if (!status.ok()) {
    LOG(ERROR) << "The cache_dir(" << mountpoint_
               << ") is not format with 3fs filesystem.";
    return status;
  }
  mountpoint_ = real_mountpoint;

  // read io ring
  status = Hf3fs::IorCreate(&ior_r_, mountpoint_, iodepth, true);
  if (!status.ok()) {
    LOG(ERROR) << "Create 3fs write io ring failed.";
    return status;
  }

  // write io ring
  status = Hf3fs::IorCreate(&ior_w_, mountpoint_, iodepth, false);
  if (!status.ok()) {
    LOG(ERROR) << "Create 3fs read io ring failed.";
    return status;
  }

  // shared memory
  status = Hf3fs::IovCreate(&iov_, mountpoint_, iodepth * blksize_);
  if (!status.ok()) {
    LOG(ERROR) << "Create 3fs shared memory failed.";
    return status;
  }

  iodepth_ = iodepth;
  cqes_ = new Hf3fs::Cqe[iodepth];
  iov_pool_ = std::make_unique<IovPool>(&iov_);
  iov_pool_->Init(blksize_, iodepth);
  return Status::OK();
}

void Usrbio::Shutdown() {
  if (!running_.exchange(false)) {
    return;
  }

  Hf3fs::IorDestroy(&ior_r_);
  Hf3fs::IorDestroy(&ior_w_);
  delete[] cqes_;
}

Status Usrbio::PrepareIo(Aio* aio) {
  bool for_read = (aio->aio_type == AioType::kRead);
  Hf3fs::Ior* ior = for_read ? &ior_r_ : &ior_w_;
  Status status =
      openfiles_->Open(aio->fd, [&](int fd) { return Hf3fs::RegFd(fd); });
  if (!status.ok()) {
    return status;
  }

  char* iov_buffer = iov_pool_->New();
  status = Hf3fs::PrepIo(ior, &iov_, for_read, iov_buffer, aio->fd, aio->offset,
                         aio->length, aio);
  if (!status.ok()) {
    iov_pool_->Delete(iov_buffer);
    openfiles_->Close(aio->fd, [&](int fd) { Hf3fs::DeRegFd(aio->fd); });
    return status;
  }

  aio->iov_buffer = iov_buffer;
  return Status::OK();
}

Status Usrbio::SubmitIo() { return Hf3fs::SubmitIos(&ior_r_); }

Status Usrbio::WaitIo(uint32_t timeout_ms, std::vector<Aio*>* aios) {
  aios->clear();

  int n = Hf3fs::WaitForIos(&ior_r_, cqes_, iodepth_, timeout_ms);
  if (n < 0) {
    return Status::Internal("hf3fs_wait_for_ios() failed");
  }

  for (int i = 0; i < n; i++) {
    auto* aio = (Aio*)(cqes_[i].userdata);
    aio->retcode = (cqes_[i].result >= 0) ? 0 : -1;
    aios->emplace_back(aio);
  }
  return Status::OK();
}

void Usrbio::PostIo(Aio* aio) {
  if (aio->aio_type == AioType::kRead) {
    memcpy(aio->buffer, aio->iov_buffer, aio->length);
    iov_pool_->Delete(aio->iov_buffer);
  }
  openfiles_->Close(aio->fd, [&](int fd) { Hf3fs::DeRegFd(aio->fd); });
}

#endif

}  // namespace utils
}  // namespace cache
}  // namespace dingofs
