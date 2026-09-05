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

#ifndef DINGOFS_BLOCKCACHE_CORE_FS_IO_RING_H_
#define DINGOFS_BLOCKCACHE_CORE_FS_IO_RING_H_

#include <glog/logging.h>
#include <liburing.h>

#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "blockcache/core/reactor/io_awaiter.h"
#include "blockcache/core/reactor/poller.h"
#include "blockcache/core/reactor/reactor.h"

namespace dingofs {
namespace blockcache {

inline constexpr uint16_t kNoBufIndex = 0xffff;

class FixedBuffers {
 public:
  explicit FixedBuffers(io_uring* ring) : ring_(ring) {}

  FixedBuffers(const FixedBuffers&) = delete;
  FixedBuffers& operator=(const FixedBuffers&) = delete;

  int Register(void* base, size_t bytes, size_t chunk);
  void Unregister();

  uint16_t IndexOf(const void* p) const {
    auto offset = static_cast<size_t>(static_cast<const char*>(p) - base_);
    return offset < bytes_ ? static_cast<uint16_t>(offset >> chunk_shift_)
                           : kNoBufIndex;
  }

  bool registered() const { return bytes_ != 0; }

 private:
  io_uring* ring_;
  const char* base_ = nullptr;
  size_t bytes_ = 0;
  unsigned chunk_shift_ = 0;
};

class FixedFiles {
 public:
  explicit FixedFiles(io_uring* ring) : ring_(ring) {}

  FixedFiles(const FixedFiles&) = delete;
  FixedFiles& operator=(const FixedFiles&) = delete;

  int Acquire(int fd);
  void Release(int slot);
  int AcquireSlot();
  void ReleaseSlot(int slot) { free_slots_.push_back(slot); }
  void Unregister();

  size_t free_slots() const { return free_slots_.size(); }

 private:
  static constexpr unsigned kSlots = 1024;

  bool EnsureRegistered();

  io_uring* ring_;
  bool registered_ = false;
  std::vector<int> free_slots_;
};

class IoRing final : public Poller {
 public:
  IoRing();
  ~IoRing() override;

  IoRing(const IoRing&) = delete;
  IoRing& operator=(const IoRing&) = delete;

  io_uring_sqe* GetSqe(IoCompletion* c);
  void ReserveSqes(unsigned n);

  bool linked_files() const {
    return (features_ & IORING_FEAT_LINKED_FILE) != 0;
  }

  bool Poll() override;
  bool PurePoll() override { return inflight_ > 0; }
  bool TryEnterInterruptMode() override { return inflight_ == 0; }
  void Flush() override { SubmitAndCollect(); }

  FixedBuffers& buffers() { return buffers_; }
  FixedFiles& files() { return files_; }

 private:
  static constexpr unsigned kCqBatch = 256;

  void Init(unsigned queue_len);
  void SubmitAndCollect();
  unsigned Reap();

  io_uring ring_;
  uint32_t features_ = 0;
  unsigned inflight_ = 0;
  bool reaping_ = false;
  FixedBuffers buffers_{&ring_};
  FixedFiles files_{&ring_};
};

inline thread_local IoRing* tls_io_ring = nullptr;

inline IoRing& ThisIoRing() {
  DCHECK(tls_io_ring != nullptr) << "no io ring on this thread";
  return *tls_io_ring;
}

inline bool HasIoRing() { return tls_io_ring != nullptr; }

template <typename Derived>
class UringAwaiter : public IoCompletion, public IoAwaiter<Derived> {
 public:
  void Complete(int32_t res) noexcept override { this->ResumeLater(res); }

 protected:
  ~UringAwaiter() = default;
  UringAwaiter() = default;

  UringAwaiter(const UringAwaiter&) = delete;
  UringAwaiter& operator=(const UringAwaiter&) = delete;
};

template <typename PrepFn>
class UringOpAwaiter final : public UringAwaiter<UringOpAwaiter<PrepFn>> {
 public:
  explicit UringOpAwaiter(PrepFn prep) : prep_(std::move(prep)) {}

  void Arm() { prep_(ThisIoRing().GetSqe(this)); }

  int32_t await_resume() const noexcept { return this->result_; }

 private:
  PrepFn prep_;
};

template <typename PrepFn>
UringOpAwaiter<PrepFn> UringOp(PrepFn prep) {
  return UringOpAwaiter<PrepFn>(std::move(prep));
}

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_CORE_FS_IO_RING_H_
