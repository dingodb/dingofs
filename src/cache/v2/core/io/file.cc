/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#include "cache/v1/core/io/file.h"

#include <fcntl.h>
#include <glog/logging.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <string>
#include <utility>

#include "cache/v1/core/reactor/reactor.h"

namespace dingofs {
namespace cache {

template <typename PrepFn>
class OpAwaiter final : public UringAwaiter<OpAwaiter<PrepFn>> {
 public:
  explicit OpAwaiter(PrepFn prep) : prep_(std::move(prep)) {}

  void Arm() { prep_(ThisIoRing().GetSqe(this)); }

  int32_t await_resume() const noexcept { return this->result_; }

 private:
  PrepFn prep_;
};

template <typename PrepFn>
static OpAwaiter<PrepFn> UringOp(PrepFn prep) {
  return OpAwaiter<PrepFn>(std::move(prep));
}

static int ToOpenFlags(OpenFlags flags) {
  int oflags = O_DIRECT | O_CLOEXEC;
  const bool r = HasFlag(flags, OpenFlags::kRead);
  const bool w = HasFlag(flags, OpenFlags::kWrite);
  oflags |= (r && w) ? O_RDWR : (w ? O_WRONLY : O_RDONLY);
  if (HasFlag(flags, OpenFlags::kCreate)) {
    oflags |= O_CREAT;
  }
  if (HasFlag(flags, OpenFlags::kTruncate)) {
    oflags |= O_TRUNC;
  }
  if (HasFlag(flags, OpenFlags::kDsync)) {
    oflags |= O_DSYNC;
  }
  return oflags;
}

RwAwaiter::RwAwaiter(File* file, bool write, uint64_t pos, void* buf,
                     uint32_t len) noexcept
    : file_(file), buf_(buf), pos_(pos), len_(len), write_(write) {}

StatusOr<size_t> RwAwaiter::await_resume() {
  if (result_ < 0) {
    return IoStatus(write_ ? "write to file" : "read from file", -result_);
  }
  return static_cast<size_t>(result_);
}

void RwAwaiter::Arm() { file_->queue_.Admit(this); }

void RwAwaiter::OnResult() noexcept { file_->queue_.OnComplete(); }

void RwAwaiter::Submit() {
  DCHECK((reinterpret_cast<uintptr_t>(buf_) & 511) == 0 && (pos_ & 511) == 0 &&
         (len_ & 511) == 0)
      << "unaligned DMA io: buf=" << buf_ << " pos=" << pos_ << " len=" << len_;

  IoRing& ring = ThisIoRing();
  io_uring_sqe* sqe = ring.GetSqe(this);
  const bool fixed_file = file_->fixed_fd_ >= 0;
  const int fd = fixed_file ? file_->fixed_fd_ : file_->fd_;
  const uint16_t buf_index = ring.buffers().IndexOf(buf_);

  if (write_) {
    if (buf_index != kNoBufIndex) {
      io_uring_prep_write_fixed(sqe, fd, buf_, len_, pos_, buf_index);
    } else {
      io_uring_prep_write(sqe, fd, buf_, len_, pos_);
    }
  } else {
    if (buf_index != kNoBufIndex) {
      io_uring_prep_read_fixed(sqe, fd, buf_, len_, pos_, buf_index);
    } else {
      io_uring_prep_read(sqe, fd, buf_, len_, pos_);
    }
  }

  if (fixed_file) {
    sqe->flags |= IOSQE_FIXED_FILE;
  }
}

RwAwaiter File::Read(uint64_t pos, void* buf, uint32_t len) {
  return {this, false, pos, buf, len};
}

RwAwaiter File::Write(uint64_t pos, const void* buf, uint32_t len) {
  return {this, true, pos, const_cast<void*>(buf), len};
}

Status IoStatus(const char* what, int err) {
  const std::string message =
      std::string("Fail to ") + what + ": " + std::strerror(err);
  switch (err) {
    case ENOENT:
      return Status::NotExist(err, message);
    case EACCES:
    case EPERM:
      return Status::NoPermission(err, message);
    case ENOSPC:
      return Status::NoSpace(err, message);
    case EINVAL:
      return Status::InvalidParam(err, message);
    case EBADF:
      return Status::BadFd(err, message);
    default:
      return Status::IoError(err, message);
  }
}

Future<StatusOr<File>> OpenFile(std::string path, OpenFlags flags,
                                OpenOption option) {
  const int oflags = ToOpenFlags(flags);
  int fd = co_await UringOp([&path, oflags, &option](io_uring_sqe* sqe) {
    io_uring_prep_openat(sqe, AT_FDCWD, path.c_str(), oflags,
                         static_cast<mode_t>(option.mode));
  });
  if (fd < 0) {
    co_return IoStatus("open file", -fd);
  }

  File file(fd, option.io_inflight);
  if (option.register_fd) {
    const int slot = ThisIoRing().files().Acquire(fd);
    if (slot >= 0) {
      file.AdoptSlot(slot);
    }
  }
  co_return file;
}

File::~File() { CloseSyncIfOpen(); }

void File::ReleaseSlot() {
  if (owns_slot_) {
    if (HasIoRing()) {
      ThisIoRing().files().Release(fixed_fd_);
    }
    fixed_fd_ = -1;
    owns_slot_ = false;
  }
}

void File::CloseSyncIfOpen() {
  if (fd_ >= 0) {
    LOG(WARNING) << "Fail to close file, fd=" << fd_
                 << " destroyed without Close(); closing synchronously";
    ReleaseSlot();
    ::close(fd_);
    fd_ = -1;
  }
}

Future<Status> File::Sync() const {
  const int fd = fd_;
  int rc = co_await UringOp([fd](io_uring_sqe* sqe) {
    io_uring_prep_fsync(sqe, fd, IORING_FSYNC_DATASYNC);
  });
  co_return rc < 0 ? IoStatus("sync file", -rc) : Status::OK();
}

Future<Status> File::Allocate(uint64_t pos, uint64_t len) const {
  const int fd = fd_;
  int rc = co_await UringOp([fd, pos, len](io_uring_sqe* sqe) {
    io_uring_prep_fallocate(sqe, fd, 0, pos, len);
  });
  co_return rc < 0 ? IoStatus("allocate file space", -rc) : Status::OK();
}

Future<StatusOr<uint64_t>> File::Size() const {
  const int fd = fd_;
  struct statx sx{};
  int rc = co_await UringOp([fd, &sx](io_uring_sqe* sqe) {
    io_uring_prep_statx(sqe, fd, "", AT_EMPTY_PATH, STATX_SIZE, &sx);
  });
  if (rc < 0) {
    co_return IoStatus("stat file", -rc);
  }
  co_return static_cast<uint64_t>(sx.stx_size);
}

Future<Status> File::Close() {
  const int fd = fd_;
  fd_ = -1;
  if (fd < 0) {
    co_return Status::OK();
  }

  ReleaseSlot();
  int rc = co_await UringOp(
      [fd](io_uring_sqe* sqe) { io_uring_prep_close(sqe, fd); });
  co_return rc < 0 ? IoStatus("close file", -rc) : Status::OK();
}

}  // namespace cache
}  // namespace dingofs
