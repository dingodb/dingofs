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

#ifndef DINGOFS_CACHE_CORE_IO_FILE_H_
#define DINGOFS_CACHE_CORE_IO_FILE_H_

#include <cstdint>
#include <string>

#include "cache/v1/core/io/io_ring.h"
#include "cache/v1/core/reactor/coroutine.h"
#include "cache/v1/core/utils/containers/admission_queue.h"
#include "cache/v1/core/utils/status.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

Status IoStatus(const char* what, int err);

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

struct OpenOption {
  uint32_t io_inflight = 128;  // per-file admission limit, on this shard
  uint32_t mode = 0644;
  bool register_fd = true;
};

class File;

class RwAwaiter final : public UringAwaiter<RwAwaiter> {
 public:
  RwAwaiter(File* file, bool write, uint64_t pos, void* buf,
            uint32_t len) noexcept;

  StatusOr<size_t> await_resume();

  void Arm();
  void OnResult() noexcept;
  void Submit();  // prep + tag the SQE

  RwAwaiter* park_next = nullptr;

 private:
  File* file_;
  void* buf_;
  uint64_t pos_;
  uint32_t len_;
  bool write_;
};

class File {
 public:
  File() = default;
  ~File();

  File(File&& o) noexcept
      : fd_(o.fd_),
        fixed_fd_(o.fixed_fd_),
        owns_slot_(o.owns_slot_),
        queue_(o.queue_) {
    o.Disown();
  }

  File& operator=(File&& o) noexcept {
    if (this != &o) {
      CloseSyncIfOpen();
      fd_ = o.fd_;
      fixed_fd_ = o.fixed_fd_;
      owns_slot_ = o.owns_slot_;
      queue_ = o.queue_;
      o.Disown();
    }
    return *this;
  }

  File(const File&) = delete;
  File& operator=(const File&) = delete;

  RwAwaiter Read(uint64_t pos, void* buf, uint32_t len);
  RwAwaiter Write(uint64_t pos, const void* buf, uint32_t len);
  Future<Status> Sync() const;
  Future<Status> Allocate(uint64_t pos, uint64_t len) const;
  Future<StatusOr<uint64_t>> Size() const;
  Future<Status> Close();

  bool Valid() const { return fd_ >= 0; }
  int fd() const { return fd_; }
  int32_t fixed_fd() const { return fixed_fd_; }
  const AdmissionQueue<RwAwaiter>& admission() const { return queue_; }

 private:
  friend class RwAwaiter;
  friend Future<StatusOr<File>> OpenFile(std::string path, OpenFlags flags,
                                         OpenOption option);

  File(int fd, uint32_t io_inflight) : fd_(fd), queue_(io_inflight) {}

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
  AdmissionQueue<RwAwaiter> queue_{128};
};

Future<StatusOr<File>> OpenFile(std::string path, OpenFlags flags,
                                OpenOption option = {});

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_CORE_IO_FILE_H_
