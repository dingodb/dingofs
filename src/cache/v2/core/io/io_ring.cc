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

#include "cache/v1/core/io/io_ring.h"

#include <glog/logging.h>
#include <sys/uio.h>

#include <cerrno>
#include <mutex>
#include <string>

namespace dingofs {
namespace cache {

struct OpcodeReq {
  int op;
  const char* name;
};

constexpr OpcodeReq kRequiredOps[] = {
    {.op = IORING_OP_NOP, .name = "NOP"},
    {.op = IORING_OP_READ, .name = "READ"},
    {.op = IORING_OP_WRITE, .name = "WRITE"},
    {.op = IORING_OP_READ_FIXED, .name = "READ_FIXED"},
    {.op = IORING_OP_WRITE_FIXED, .name = "WRITE_FIXED"},
    {.op = IORING_OP_FSYNC, .name = "FSYNC"},
    {.op = IORING_OP_FALLOCATE, .name = "FALLOCATE"},
    {.op = IORING_OP_OPENAT, .name = "OPENAT"},
    {.op = IORING_OP_STATX, .name = "STATX"},
    {.op = IORING_OP_CLOSE, .name = "CLOSE"},
    {.op = IORING_OP_ASYNC_CANCEL, .name = "ASYNC_CANCEL"},
};

constexpr unsigned kSetupLadder[] = {
    IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN |
        IORING_SETUP_TASKRUN_FLAG,
    IORING_SETUP_COOP_TASKRUN | IORING_SETUP_TASKRUN_FLAG,
    0,
};

static void CheckOpCodes(io_uring* ring) {
  static std::once_flag once;
  std::call_once(once, [ring] {
    io_uring_probe* probe = io_uring_get_probe_ring(ring);
    if (probe == nullptr) {
      return;
    }

    std::string missing;
    for (const auto& req : kRequiredOps) {
      if (!io_uring_opcode_supported(probe, req.op)) {
        missing += req.name;
        missing += ' ';
      }
    }

    io_uring_free_probe(probe);
    if (!missing.empty()) {
      LOG(FATAL) << "Fail to use io_uring, missing opcodes: " << missing;
    }
  });
}

int FixedBuffers::Register(void* base, size_t bytes, size_t chunk) {
  CHECK(!registered()) << "one registered region at a time";
  CHECK(chunk != 0 && (chunk & (chunk - 1)) == 0) << "chunk must be 2^n";
  CHECK(bytes != 0 && bytes % chunk == 0) << "chunk must divide the region";

  const size_t count = bytes / chunk;
  std::vector<iovec> iovs(count);
  for (size_t i = 0; i < count; ++i) {
    iovs[i] = {.iov_base = static_cast<char*>(base) + (i * chunk),
               .iov_len = chunk};
  }

  int rc = io_uring_register_buffers(ring_, iovs.data(),
                                     static_cast<unsigned>(count));
  if (rc < 0) {
    PLOG(ERROR) << "Fail to register buffers";
    return rc;
  }

  base_ = static_cast<const char*>(base);
  bytes_ = bytes;
  chunk_shift_ = static_cast<unsigned>(__builtin_ctzll(chunk));
  return 0;
}

void FixedBuffers::Unregister() {
  if (!registered()) {
    return;
  }

  io_uring_unregister_buffers(ring_);
  base_ = nullptr;
  bytes_ = 0;
  chunk_shift_ = 0;
}

int FixedFiles::Acquire(int fd) {
  if (!registered_) {
    std::vector<int> sparse(kSlots, -1);
    if (io_uring_register_files(ring_, sparse.data(), kSlots) < 0) {
      return -1;
    }

    registered_ = true;
    free_slots_.reserve(kSlots);
    for (unsigned i = kSlots; i > 0; --i) {
      free_slots_.push_back(static_cast<int>(i - 1));
    }
  }

  if (free_slots_.empty()) {
    return -1;
  }

  const int slot = free_slots_.back();
  if (io_uring_register_files_update(ring_, static_cast<unsigned>(slot), &fd,
                                     1) < 0) {
    return -1;
  }

  free_slots_.pop_back();
  return slot;
}

void FixedFiles::Release(int slot) {
  DCHECK(registered_);
  int none = -1;
  io_uring_register_files_update(ring_, static_cast<unsigned>(slot), &none, 1);
  free_slots_.push_back(slot);
}

void FixedFiles::Unregister() {
  if (!registered_) {
    return;
  }

  io_uring_unregister_files(ring_);
  registered_ = false;
  free_slots_.clear();
}

IoRing::IoRing(const IoRingOption& option) {
  Init(option.queue_len);
  CheckOpCodes(&ring_);
  CHECK(tls_io_ring == nullptr) << "one io ring per shard";
  tls_io_ring = this;
}

IoRing::~IoRing() {
  Reap();                 // best-effort: consume already-finished CQEs
  buffers_.Unregister();  // both give their tables back while the ring lives
  files_.Unregister();
  io_uring_queue_exit(&ring_);
  tls_io_ring = nullptr;
}

void IoRing::Init(unsigned queue_len) {
  io_uring_params params{};
  int rc = -EINVAL;
  for (unsigned flags : kSetupLadder) {
    params = io_uring_params{};
    params.flags = flags;
    rc = io_uring_queue_init_params(queue_len, &ring_, &params);
    if (rc != -EINVAL) {
      break;
    }
  }

  if (rc < 0) {
    errno = -rc;
    PLOG(FATAL) << "Fail to io_uring_queue_init_params(io)";
  }

  constexpr uint32_t kNeeded = IORING_FEAT_SUBMIT_STABLE | IORING_FEAT_NODROP;
  if ((params.features & kNeeded) != kNeeded) {
    io_uring_queue_exit(&ring_);
    LOG(FATAL) << "Fail to use io_uring: kernel lacks SUBMIT_STABLE|NODROP";
  }

  (void)io_uring_register_ring_fd(&ring_);
  io_uring_ring_dontfork(&ring_);
}

io_uring_sqe* IoRing::GetSqe(IoCompletion* c) {
  io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  while (__builtin_expect(sqe == nullptr, false)) {
    SubmitAndCollect();
    sqe = io_uring_get_sqe(&ring_);
    if (sqe != nullptr) {
      break;
    }

    Reap();
    sqe = io_uring_get_sqe(&ring_);
  }

  ++inflight_;
  io_uring_sqe_set_data(sqe, static_cast<void*>(c));
  return sqe;
}

void IoRing::SubmitAndCollect() {
  if (io_uring_sq_ready(&ring_) == 0) {
    return;
  }

  int rc = io_uring_submit_and_get_events(&ring_);
  if (rc < 0) {
    if (rc == -EBUSY || rc == -EAGAIN) {
      return;  // CQ full / resources
    }

    errno = -rc;
    PLOG(FATAL) << "Fail to io_uring_submit_and_get_events(io)";
  }
}

bool IoRing::Poll() {
  if (inflight_ == 0) {
    return false;
  }

  SubmitAndCollect();
  return Reap() > 0;
}

unsigned IoRing::Reap() {
  if (reaping_) {
    return 0;
  }
  reaping_ = true;

  unsigned total = 0;
  io_uring_cqe* cqes[kCqBatch];
  for (;;) {
    unsigned n = io_uring_peek_batch_cqe(&ring_, cqes, kCqBatch);
    if (n == 0) {
      break;
    }

    for (unsigned i = 0; i < n; ++i) {
      if (i + 3 < n) {
        __builtin_prefetch(reinterpret_cast<void*>(
            static_cast<uintptr_t>(cqes[i + 3]->user_data)));
      }

      auto* c = static_cast<IoCompletion*>(io_uring_cqe_get_data(cqes[i]));
      c->Complete(cqes[i]->res);
    }

    io_uring_cq_advance(&ring_, n);
    total += n;
    if (n < kCqBatch) {
      break;
    }
  }

  inflight_ -= total;
  reaping_ = false;
  return total;
}

}  // namespace cache
}  // namespace dingofs
