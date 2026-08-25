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

#include "blockcache/core/fs/filesystem.h"

#include <fcntl.h>
#include <glog/logging.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <string>
#include <utility>

#include "blockcache/common/status.h"
#include "blockcache/core/reactor/reactor.h"
#include "blockcache/utils/align.h"

namespace dingofs {
namespace blockcache {

static bool IsOpSupported(int op) {
  static io_uring_probe* probe = io_uring_get_probe();
  return probe != nullptr && io_uring_opcode_supported(probe, op);
}

static Future<int> Mkdir(std::string path, uint32_t mode) {
  if (IsOpSupported(IORING_OP_MKDIRAT)) {
    co_return co_await UringOp([&path, mode](io_uring_sqe* sqe) {
      io_uring_prep_mkdirat(sqe, AT_FDCWD, path.c_str(),
                            static_cast<mode_t>(mode));
    });
  }
  LOG_FIRST_N(WARNING, 1) << "io_uring lacks MKDIRAT, using sync fallback";
  co_return ::mkdir(path.c_str(), static_cast<mode_t>(mode)) == 0 ? 0 : -errno;
}

static int ToOpenFlags(OpenFlags flags, bool direct) {
  int oflags = O_CLOEXEC | (direct ? O_DIRECT : 0);
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

RwAwaiter::RwAwaiter(File* file, bool write, uint64_t pos, void* buffer,
                     uint32_t len) noexcept
    : file_(file), buffer_(buffer), pos_(pos), len_(len), write_(write) {}

RwAwaiter::RwAwaiter(File* file, uint64_t pos, const struct iovec* iov,
                     unsigned iovcnt, uint32_t len) noexcept
    : file_(file),
      buffer_(nullptr),
      iov_(iov),
      pos_(pos),
      len_(len),
      iovcnt_(iovcnt),
      write_(true) {}

StatusOr<size_t> RwAwaiter::await_resume() {
  if (result_ < 0) {
    return ToStatus(-result_, write_ ? "write to file" : "read from file");
  }
  return static_cast<size_t>(result_);
}

void RwAwaiter::Arm() { file_->queue_.Admit(this); }

void RwAwaiter::OnResult() noexcept { file_->queue_.OnComplete(); }

void RwAwaiter::Submit() {
  IoRing& ring = ThisIoRing();
  io_uring_sqe* sqe = ring.GetSqe(this);
  const bool fixed_file = file_->fixed_fd_ >= 0;
  const int fd = fixed_file ? file_->fixed_fd_ : file_->fd_;

  if (iov_ != nullptr) {
    DCHECK(!file_->direct() || IsAligned(pos_, kDirectIoAlign))
        << "unaligned DMA writev: pos=" << pos_;
    io_uring_prep_writev(sqe, fd, iov_, iovcnt_, pos_);
    if (fixed_file) {
      sqe->flags |= IOSQE_FIXED_FILE;
    }
    return;
  }

  DCHECK(!file_->direct() ||
         (IsAligned(buffer_, kDirectIoAlign) &&
          IsAligned(pos_, kDirectIoAlign) && IsAligned(len_, kDirectIoAlign)))
      << "unaligned DMA io: buffer=" << buffer_ << " pos=" << pos_
      << " len=" << len_;

  const uint16_t buffer_index = ring.buffers().IndexOf(buffer_);

  if (write_) {
    if (buffer_index != kNoBufIndex) {
      io_uring_prep_write_fixed(sqe, fd, buffer_, len_, pos_, buffer_index);
    } else {
      io_uring_prep_write(sqe, fd, buffer_, len_, pos_);
    }
  } else {
    if (buffer_index != kNoBufIndex) {
      io_uring_prep_read_fixed(sqe, fd, buffer_, len_, pos_, buffer_index);
    } else {
      io_uring_prep_read(sqe, fd, buffer_, len_, pos_);
    }
  }

  if (fixed_file) {
    sqe->flags |= IOSQE_FIXED_FILE;
  }
}

File::~File() { CloseSyncIfOpen(); }

Future<Status> File::Close() {
  const int fd = fd_;
  fd_ = -1;
  if (fd < 0) {
    co_return Status::OK();
  }

  ReleaseSlot();
  int rc = co_await UringOp(
      [fd](io_uring_sqe* sqe) { io_uring_prep_close(sqe, fd); });
  if (rc < 0) {
    co_return ToStatus(-rc, "close file");
  }
  co_return Status::OK();
}

RwAwaiter File::Read(uint64_t pos, void* buffer, uint32_t len) {
  return {this, false, pos, buffer, len};
}

RwAwaiter File::Write(uint64_t pos, const void* buffer, uint32_t len) {
  return {this, true, pos, const_cast<void*>(buffer), len};
}

RwAwaiter File::Writev(uint64_t pos, const struct iovec* iov, unsigned iovcnt,
                       uint32_t len) {
  return {this, pos, iov, iovcnt, len};
}

Future<Status> File::Sync() const {
  const int fd = fd_;
  int rc = co_await UringOp([fd](io_uring_sqe* sqe) {
    io_uring_prep_fsync(sqe, fd, IORING_FSYNC_DATASYNC);
  });
  if (rc < 0) {
    co_return ToStatus(-rc, "sync file");
  }
  co_return Status::OK();
}

Future<Status> File::Allocate(uint64_t pos, uint64_t len) const {
  const int fd = fd_;
  int rc = co_await UringOp([fd, pos, len](io_uring_sqe* sqe) {
    io_uring_prep_fallocate(sqe, fd, 0, pos, len);
  });
  if (rc < 0) {
    co_return ToStatus(-rc, "allocate file space");
  }
  co_return Status::OK();
}

Future<StatusOr<uint64_t>> File::Size() const {
  const int fd = fd_;
  struct statx sx{};
  int rc = co_await UringOp([fd, &sx](io_uring_sqe* sqe) {
    io_uring_prep_statx(sqe, fd, "", AT_EMPTY_PATH, STATX_SIZE, &sx);
  });
  if (rc < 0) {
    co_return ToStatus(-rc, "stat file");
  }
  co_return static_cast<uint64_t>(sx.stx_size);
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

void File::ReleaseSlot() {
  if (owns_slot_) {
    if (HasIoRing()) {
      ThisIoRing().files().Release(fixed_fd_);
    }
    fixed_fd_ = -1;
    owns_slot_ = false;
  }
}

Future<StatusOr<File>> FileSystem::Open(std::string path, OpenFlags flags,
                                        OpenOption option) {
  const int oflags = ToOpenFlags(flags, option.direct);
  int fd = co_await UringOp([&path, oflags, &option](io_uring_sqe* sqe) {
    io_uring_prep_openat(sqe, AT_FDCWD, path.c_str(), oflags,
                         static_cast<mode_t>(option.mode));
  });
  if (fd < 0) {
    co_return ToStatus(-fd, "open file");
  }

  if (!option.direct) {
    // Buffered means the kernel decides how much to read; without this it
    // would read ahead into a page cache we do not want in the first place.
    (void)posix_fadvise(fd, 0, 0, POSIX_FADV_RANDOM);
  }

  File file(fd, option.io_inflight, option.direct);
  if (option.register_fd) {
    const int slot = ThisIoRing().files().Acquire(fd);
    if (slot >= 0) {
      file.AdoptSlot(slot);
    }
  }
  co_return file;
}

Future<Status> FileSystem::Unlink(std::string path) {
  int rc;
  if (IsOpSupported(IORING_OP_UNLINKAT)) {
    rc = co_await UringOp([&path](io_uring_sqe* sqe) {
      io_uring_prep_unlinkat(sqe, AT_FDCWD, path.c_str(), 0);
    });
  } else {
    LOG_FIRST_N(WARNING, 1) << "io_uring lacks UNLINKAT, using sync fallback";
    rc = ::unlink(path.c_str()) == 0 ? 0 : -errno;
  }
  if (rc < 0) {
    co_return ToStatus(-rc, "unlink file");
  }
  co_return Status::OK();
}

Future<Status> FileSystem::Link(std::string from, std::string to) {
  int rc;
  if (IsOpSupported(IORING_OP_LINKAT)) {
    rc = co_await UringOp([&from, &to](io_uring_sqe* sqe) {
      io_uring_prep_linkat(sqe, AT_FDCWD, from.c_str(), AT_FDCWD, to.c_str(),
                           0);
    });
  } else {
    LOG_FIRST_N(WARNING, 1) << "io_uring lacks LINKAT, using sync fallback";
    rc = ::link(from.c_str(), to.c_str()) == 0 ? 0 : -errno;
  }
  if (rc < 0) {
    co_return ToStatus(-rc, "link file");
  }
  co_return Status::OK();
}

Future<Status> FileSystem::Rename(std::string from, std::string to) {
  int rc;
  if (IsOpSupported(IORING_OP_RENAMEAT)) {
    rc = co_await UringOp([&from, &to](io_uring_sqe* sqe) {
      io_uring_prep_renameat(sqe, AT_FDCWD, from.c_str(), AT_FDCWD, to.c_str(),
                             0);
    });
  } else {
    LOG_FIRST_N(WARNING, 1) << "io_uring lacks RENAMEAT, using sync fallback";
    rc = ::rename(from.c_str(), to.c_str()) == 0 ? 0 : -errno;
  }
  if (rc < 0) {
    co_return ToStatus(-rc, "rename file");
  }
  co_return Status::OK();
}

Future<Status> FileSystem::MakeDirs(std::string path, uint32_t mode) {
  int rc = co_await Mkdir(path, mode);
  if (rc == 0 || rc == -EEXIST) {
    co_return Status::OK();
  }
  if (rc != -ENOENT) {
    co_return ToStatus(-rc, "make directory");
  }

  const auto sep = path.find_last_of('/');
  if (sep == std::string::npos || sep == 0) {
    co_return ToStatus(ENOENT, "make directory");
  }
  const Status status = co_await MakeDirs(path.substr(0, sep), mode);
  if (!status.ok()) {
    co_return status;
  }

  rc = co_await Mkdir(path, mode);
  co_return (rc == 0 || rc == -EEXIST) ? Status::OK()
                                       : ToStatus(-rc, "make directory");
}

Future<StatusOr<FileStat>> FileSystem::StatPath(std::string path) {
  constexpr unsigned kMask =
      STATX_SIZE | STATX_NLINK | STATX_ATIME | STATX_MTIME;
  struct statx sx{};
  int rc;
  if (IsOpSupported(IORING_OP_STATX)) {
    rc = co_await UringOp([&path, &sx](io_uring_sqe* sqe) {
      io_uring_prep_statx(sqe, AT_FDCWD, path.c_str(), 0, kMask, &sx);
    });
  } else {
    LOG_FIRST_N(WARNING, 1) << "io_uring lacks STATX, using sync fallback";
    rc = ::statx(AT_FDCWD, path.c_str(), 0, kMask, &sx) == 0 ? 0 : -errno;
  }
  if (rc < 0) {
    co_return ToStatus(-rc, "stat path");
  }
  co_return FileStat{.size = sx.stx_size,
                     .nlink = sx.stx_nlink,
                     .atime_sec = sx.stx_atime.tv_sec};
}

}  // namespace blockcache
}  // namespace dingofs
