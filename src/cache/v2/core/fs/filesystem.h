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

#ifndef DINGOFS_CACHE_V2_CORE_FS_FILESYSTEM_H_
#define DINGOFS_CACHE_V2_CORE_FS_FILESYSTEM_H_

#include <cstdint>
#include <string>

#include "cache/v2/common/status.h"
#include "cache/v2/core/fs/io_ring.h"
#include "cache/v2/core/reactor/coroutine.h"
#include "cache/v2/utils/containers/admission_queue.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace v2 {

enum class OpenFlags : uint8_t {
  kRead = 1u << 0,
  kWrite = 1u << 1,
  kCreate = 1u << 2,
  kTruncate = 1u << 3,
  kDsync = 1u << 4,
};

inline OpenFlags operator|(OpenFlags a, OpenFlags b) {
  return static_cast<OpenFlags>(static_cast<uint32_t>(a) |
                                static_cast<uint32_t>(b));
}

inline bool HasFlag(OpenFlags v, OpenFlags f) {
  return (static_cast<uint32_t>(v) & static_cast<uint32_t>(f)) != 0;
}

struct FileStat {
  uint64_t size = 0;
  uint32_t nlink = 0;
  int64_t atime_sec = 0;
  int64_t mtime_sec = 0;
};

struct OpenOption {
  uint32_t io_inflight = 128;  // per-file admission limit, on this shard
  uint32_t mode = 0644;
  bool register_fd = true;
  // O_DIRECT, and with it the 4 KiB alignment every buffer must satisfy.
  // Clearing it hands an unaligned transfer to the kernel instead: the copy
  // still happens, but page-cache misses run it on an io-wq thread rather
  // than on the shard, and we write no bounce code for it. Reads only --
  // a buffered write would sit in the page cache and be lost on a crash.
  bool direct = true;
};

class File;

class RwAwaiter final : public UringAwaiter<RwAwaiter> {
 public:
  RwAwaiter(File* file, bool write, uint64_t pos, void* buffer,
            uint32_t len) noexcept;
  // Vectored write. `iov` must outlive the await; `len` is its total, kept
  // separately so the short-write check reads the same in both modes.
  RwAwaiter(File* file, uint64_t pos, const struct iovec* iov, unsigned iovcnt,
            uint32_t len) noexcept;

  StatusOr<size_t> await_resume();

  void Arm();
  void OnResult() noexcept;
  void Submit();  // prep + tag the SQE

  RwAwaiter* park_next = nullptr;

 private:
  File* file_;
  void* buffer_;                       // scalar mode
  const struct iovec* iov_ = nullptr;  // vectored mode, when non-null
  uint64_t pos_;
  uint32_t len_;
  unsigned iovcnt_ = 0;
  bool write_;
};

class File {
 public:
  File() = default;
  ~File();

  File(const File&) = delete;
  File& operator=(const File&) = delete;

  File(File&& o) noexcept
      : fd_(o.fd_),
        fixed_fd_(o.fixed_fd_),
        owns_slot_(o.owns_slot_),
        direct_(o.direct_),
        queue_(o.queue_) {
    o.Disown();
  }

  File& operator=(File&& o) noexcept {
    if (this != &o) {
      CloseSyncIfOpen();
      fd_ = o.fd_;
      fixed_fd_ = o.fixed_fd_;
      owns_slot_ = o.owns_slot_;
      direct_ = o.direct_;
      queue_ = o.queue_;
      o.Disown();
    }
    return *this;
  }

  Future<Status> Close();

  RwAwaiter Read(uint64_t pos, void* buffer, uint32_t len);
  RwAwaiter Write(uint64_t pos, const void* buffer, uint32_t len);
  // One io for a body that arrived in pieces. No fixed-buffer variant
  // exists for vectored io, so a single-range body should use Write.
  RwAwaiter Writev(uint64_t pos, const struct iovec* iov, unsigned iovcnt,
                   uint32_t len);
  Future<Status> Sync() const;
  Future<Status> Allocate(uint64_t pos, uint64_t len) const;
  Future<StatusOr<uint64_t>> Size() const;

  bool Valid() const { return fd_ >= 0; }
  int fd() const { return fd_; }
  bool direct() const { return direct_; }
  int32_t fixed_fd() const { return fixed_fd_; }
  const AdmissionQueue<RwAwaiter>& admission() const { return queue_; }

 private:
  friend class RwAwaiter;
  friend class FileSystem;

  File(int fd, uint32_t io_inflight, bool direct)
      : fd_(fd), direct_(direct), queue_(io_inflight) {}

  void AdoptSlot(int32_t slot) {
    fixed_fd_ = slot;
    owns_slot_ = true;
  }

  void CloseSyncIfOpen();
  void ReleaseSlot();  // no-op unless a slot is owned
  void Disown() {
    fd_ = -1;
    fixed_fd_ = -1;
    owns_slot_ = false;
  }

  int fd_ = -1;
  int32_t fixed_fd_ = -1;
  bool owns_slot_ = false;
  bool direct_ = true;
  AdmissionQueue<RwAwaiter> queue_{128};
};

// Async fs ops on this shard's io_uring; missing opcodes fall back to syscall.
class FileSystem {
 public:
  FileSystem() = delete;

  static Future<StatusOr<File>> Open(std::string path, OpenFlags flags,
                                     OpenOption option = {});

  static Future<Status> Unlink(std::string path);
  static Future<Status> Link(std::string from, std::string to);
  static Future<Status> Rename(std::string from, std::string to);

  // mkdir -p; an existing dir at any level is success (cross-shard race-free).
  static Future<Status> MakeDirs(std::string path, uint32_t mode = 0755);

  // Stat by path; NotExist when absent.
  static Future<StatusOr<FileStat>> StatPath(std::string path);
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_FS_FILESYSTEM_H_
