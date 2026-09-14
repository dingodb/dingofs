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
#include "blockcache/utils/containers/park_queue.h"

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

// Wants `slots` SQEs; Issue() runs once the ring has CQ room for them and
// must take exactly that many with GetSqe().
class RingOp {
 public:
  virtual void Issue() = 0;

  RingOp* park_next = nullptr;
  unsigned slots = 1;

 protected:
  ~RingOp() = default;
  RingOp() = default;

  RingOp(const RingOp&) = delete;
  RingOp& operator=(const RingOp&) = delete;
};

class IoRing final : public Poller {
 public:
  IoRing();
  ~IoRing() override;

  IoRing(const IoRing&) = delete;
  IoRing& operator=(const IoRing&) = delete;

  // Keeps in-flight requests within the CQ so it can never overflow: an
  // overflowed CQ stalls completions, and on Ubuntu 5.15.0-178+ kernels it
  // leaks uring_lock and hangs the shard for good (CVE-2024-50060 backport).
  void Admit(RingOp* op, unsigned slots = 1);
  io_uring_sqe* GetSqe(IoCompletion* c);  // only from RingOp::Issue()
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

  unsigned inflight() const { return inflight_; }
  unsigned cq_capacity() const { return cq_capacity_; }
  unsigned peak_inflight() const { return peak_inflight_; }
  unsigned parked() const { return parked_.size(); }
  uint64_t deferred() const { return deferred_; }

 private:
  static constexpr unsigned kCqBatch = 256;
  static constexpr unsigned kCqPerSq = 4;
  static constexpr unsigned kMaxCqEntries = 1u << 16;  // kernel ceiling

  void Init(unsigned queue_len);
  void Issue(RingOp* op);
  void SubmitAndCollect();
  unsigned Reap();
  void DrainParked();

  io_uring ring_;
  uint32_t features_ = 0;
  unsigned inflight_ = 0;
  unsigned cq_capacity_ = 0;
  unsigned peak_inflight_ = 0;
  uint64_t deferred_ = 0;
  bool reaping_ = false;
  ParkQueue<RingOp> parked_;
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
class UringOpAwaiter final : public UringAwaiter<UringOpAwaiter<PrepFn>>,
                             public RingOp {
 public:
  explicit UringOpAwaiter(PrepFn prep) : prep_(std::move(prep)) {}

  void Arm() { ThisIoRing().Admit(this); }
  void Issue() override { prep_(ThisIoRing().GetSqe(this)); }

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
