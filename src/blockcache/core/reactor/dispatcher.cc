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

#include "blockcache/core/reactor/dispatcher.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <mutex>
#include <string>

#include "blockcache/utils/time.h"

namespace dingofs {
namespace blockcache {

static bool Positive(const char* /*name*/, uint32_t value) { return value > 0; }

DEFINE_uint32(task_quota_us, 500,
              "microseconds a task may run before the reactor preempts it");
DEFINE_validator(task_quota_us, Positive);

struct UringOpcode {
  int op;
  const char* name;
};

constexpr UringOpcode kRequiredOps[] = {
    {.op = IORING_OP_READ, .name = "READ"},
    {.op = IORING_OP_POLL_ADD, .name = "POLL_ADD"},
    {.op = IORING_OP_ASYNC_CANCEL, .name = "ASYNC_CANCEL"},
};

static inline void PrefetchEvent(const io_uring_cqe* cqe) {
  __builtin_prefetch(
      reinterpret_cast<const void*>(static_cast<uintptr_t>(cqe->user_data)));
}

// This and CheckOpcodes() both LOG(FATAL) on a kernel that cannot serve the
// ring: that is a deployment error, not something to degrade around.
static void CheckFeatures(io_uring* ring, const io_uring_params& params) {
  constexpr uint32_t kNeeded = IORING_FEAT_SUBMIT_STABLE | IORING_FEAT_NODROP;
  if ((params.features & kNeeded) != kNeeded) {
    io_uring_queue_exit(ring);
    LOG(FATAL) << "Fail to use io_uring: kernel lacks SUBMIT_STABLE|NODROP";
  }
}

EventRing::EventRing(unsigned queue_len) {
  Init(queue_len);
  CheckOpcodes();
}

void EventRing::Exit() {
  if (open_) {
    open_ = false;
    io_uring_queue_exit(&ring_);
  }
}

void EventRing::Submit() {
  if (io_uring_sq_ready(&ring_) == 0) {
    return;
  }

  int rc = io_uring_submit(&ring_);
  if (rc < 0) {
    if (rc == -EBUSY || rc == -EAGAIN) {
      return;  // CQ full / resources
    }

    errno = -rc;
    PLOG(FATAL) << "Fail to io_uring_submit";
  }
}

void EventRing::SubmitAndWait() {
  int rc = io_uring_submit_and_wait(&ring_, 1);
  if (rc < 0 && rc != -EINTR) {
    errno = -rc;
    PLOG(FATAL) << "Fail to io_uring_submit_and_wait";
  }
}

PreemptMonitor EventRing::Monitor() const {
  return {.head = ring_.cq.khead,
          .tail = ring_.cq.ktail,
          .flags = ring_.sq.kflags,
          .flag_mask = IORING_SQ_CQ_OVERFLOW};
}

void EventRing::Init(unsigned queue_len) {
  io_uring_params params{};
  int rc = io_uring_queue_init_params(queue_len, &ring_, &params);
  if (rc < 0) {
    errno = -rc;
    PLOG(FATAL) << "Fail to io_uring_queue_init_params";
  }

  CheckFeatures(&ring_, params);

  open_ = true;
  io_uring_ring_dontfork(&ring_);
}

void EventRing::CheckOpcodes() {
  static std::once_flag once;
  std::call_once(once, [this] {
    io_uring_probe* probe = io_uring_get_probe_ring(&ring_);
    if (probe == nullptr) {
      return;
    }

    std::string missing;
    for (const UringOpcode& op : kRequiredOps) {
      if (!io_uring_opcode_supported(probe, op.op)) {
        missing += op.name;
        missing += ' ';
      }
    }

    io_uring_free_probe(probe);
    if (!missing.empty()) {
      LOG(FATAL) << "Fail to use io_uring: missing opcodes: " << missing;
    }
  });
}

QuotaTicker::QuotaTicker(std::chrono::microseconds quota) : quota_(quota) {
  timerfd_ = ::timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC | TFD_NONBLOCK);
  if (timerfd_ < 0) {
    PLOG(FATAL) << "Fail to timerfd_create";
  }
}

QuotaTicker::~QuotaTicker() {
  if (timerfd_ >= 0) {
    ::close(timerfd_);
  }
}

void QuotaTicker::Arm() const {
  auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(quota_);
  itimerspec its{};
  its.it_value.tv_sec = ns.count() / kNsPerSec;
  its.it_value.tv_nsec = ns.count() % kNsPerSec;
  its.it_interval = its.it_value;
  if (::timerfd_settime(timerfd_, 0, &its, nullptr) < 0) {
    PLOG(FATAL) << "Fail to timerfd_settime(start)";
  }
}

void QuotaTicker::Disarm() const {
  itimerspec zero{};
  if (::timerfd_settime(timerfd_, 0, &zero, nullptr) < 0) {
    PLOG(FATAL) << "Fail to timerfd_settime(stop)";
  }
}

Waker::Waker() {
  eventfd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  if (eventfd_ < 0) {
    PLOG(FATAL) << "Fail to eventfd";
  }
}

Waker::~Waker() {
  if (eventfd_ >= 0) {
    ::close(eventfd_);
  }
}

void Waker::Ring() const {
  uint64_t one = 1;
  ssize_t rc = ::write(eventfd_, &one, sizeof(one));
  (void)rc;
}

Dispatcher::RunScope::RunScope(Dispatcher& dispatcher)
    : dispatcher_(dispatcher) {
  CHECK(tls_preempt_monitor.head == &kNeverPreempt)
      << "one preempt monitor per thread";
  tls_preempt_monitor = dispatcher.ring_.Monitor();
  dispatcher_.ticker_.Arm();
  dispatcher_.ring_.Submit();
}

Dispatcher::RunScope::~RunScope() {
  dispatcher_.ticker_.Disarm();
  dispatcher_.ProcessEvents();
  tls_preempt_monitor = {};
}

Dispatcher::Dispatcher()
    : ring_(kEventRingLen),
      ticker_(std::chrono::microseconds(FLAGS_task_quota_us)) {
  CHECK(tls_dispatcher == nullptr) << "one dispatcher per thread";
  ticker_.quiet_ = true;
  AddEvent(ticker_.fd(), &ticker_, EventMode::kReadRepeat);
  AddEvent(waker_.fd(), &waker_, EventMode::kReadRepeat);
  tls_dispatcher = this;
}

Dispatcher::~Dispatcher() {
  tls_dispatcher = nullptr;
  ring_.Exit();  // torn down first: no in-flight SQE can outlive its Event
  ForceIdle(&ticker_);
  ForceIdle(&waker_);
}

void Dispatcher::AddEvent(int fd, Event* ev, EventMode mode) {
  DCHECK(ev->state_ == Event::State::kIdle) << "Event is already registered";
  ev->fd_ = fd;
  ev->mode_ = mode;
  ev->notify_ = false;
  Arm(ev);
}

bool Dispatcher::DeleteEvent(Event* ev) {
  if (ev->state_ == Event::State::kIdle) {
    return false;  // never registered, or already retired: nothing is coming
  }

  ev->notify_ = true;
  Disarm(ev);
  return true;
}

void Dispatcher::DeleteEventAndWait(Event* ev) {
  DCHECK(!dispatching_) << "Detach from inside a handler; use DeleteEvent()";
  DCHECK(ev->state_ != Event::State::kPendingRearm)
      << "Detach of an event mid-batch; use DeleteEvent() instead";
  if (ev->state_ == Event::State::kIdle) {
    return;
  }

  ev->notify_ = false;
  Disarm(ev);

  while (ev->state_ != Event::State::kIdle) {
    ProcessEvents();  // submits the cancel on its way out, then reaps
    if (ev->state_ != Event::State::kIdle) {
      ring_.SubmitAndWait();
    }
  }
}

unsigned Dispatcher::ProcessEvents() {
  if (dispatching_) {
    return 0;
  }

  dispatching_ = true;

  unsigned work = 0;
  io_uring_cqe* cqes[kCqBatch];
  for (;;) {
    unsigned n = ring_.PeekBatch(cqes, kCqBatch);
    if (n == 0) {
      break;
    }

    // One clock read serves every handler in this batch.
    RefreshCachedTimestamp();

    for (unsigned i = 0; i < n; ++i) {
      if (i + 3 < n) {
        PrefetchEvent(cqes[i + 3]);
      }

      auto* ev = static_cast<Event*>(io_uring_cqe_get_data(cqes[i]));
      if (ev != nullptr) {  // null user_data: a cancel's own completion
        work += Dispatch(ev) ? 1 : 0;
      }
    }

    ring_.Advance(n);
    if (n < kCqBatch) {
      break;
    }
  }

  dispatching_ = false;

  if (!rearm_.empty()) {
    DrainRearm();
  }

  if (!cancelled_.empty()) {
    DrainCancelled();
  }

  if (ring_.HasUnsubmitted()) {
    ring_.Submit();
  }
  return work;
}

void Dispatcher::WaitForEvent() {
  ticker_.Disarm();  // or the quota tick would wake us for nothing
  ring_.SubmitAndWait();
  ticker_.Arm();
}

void Dispatcher::Arm(Event* ev) {
  io_uring_sqe* sqe = GetSqe();
  if (ev->mode_ == EventMode::kReadRepeat) {
    io_uring_prep_read(sqe, ev->fd_, &ev->buffer_, sizeof(ev->buffer_), 0);
  } else {
    io_uring_prep_poll_add(sqe, ev->fd_, POLLIN);
  }

  io_uring_sqe_set_data(sqe, static_cast<void*>(ev));
  ev->state_ = Event::State::kArmed;
}

void Dispatcher::Disarm(Event* ev) {
  if (ev->state_ == Event::State::kPendingRearm) {
    ev->state_ = Event::State::kCancelling;
    return;
  }

  if (ev->state_ != Event::State::kArmed) {
    return;
  }

  ev->state_ = Event::State::kCancelling;
  io_uring_sqe* sqe = GetSqe();
  io_uring_prep_cancel(sqe, static_cast<void*>(ev), 0);
  io_uring_sqe_set_data64(sqe, 0);
}

bool Dispatcher::Dispatch(Event* ev) noexcept {
  if (ev->state_ == Event::State::kCancelling) {
    cancelled_.Push(ev);  // the -ECANCELED we asked for
    return false;
  }

  DCHECK(ev->state_ == Event::State::kArmed)
      << "completion for an unarmed event";

  if (ev->mode_ == EventMode::kReadRepeat) {
    ev->state_ = Event::State::kPendingRearm;
    rearm_.Push(ev);
  } else {
    ev->state_ = Event::State::kIdle;  // one-shot: this registration is over
    ev->fd_ = -1;
  }

  const bool work = !ev->quiet_;
  ev->OnReady();
  return work;
}

void Dispatcher::DrainRearm() {
  EventList batch = rearm_.TakeAll();
  while (Event* ev = batch.PopFront()) {
    if (ev->state_ == Event::State::kCancelling) {
      cancelled_.Push(ev);  // cancelled after it fired: nothing to re-arm
    } else {
      Arm(ev);
    }
  }
}

void Dispatcher::DrainCancelled() {
  EventList batch = cancelled_.TakeAll();
  while (Event* ev = batch.PopFront()) {  // OnCancelled() may destroy ev
    ev->state_ = Event::State::kIdle;
    ev->fd_ = -1;
    const bool notify = ev->notify_;
    ev->notify_ = false;
    if (notify) {
      ev->OnCancelled();
    }
  }
}

io_uring_sqe* Dispatcher::GetSqe() {
  io_uring_sqe* sqe = ring_.TryGetSqe();
  while (__builtin_expect(sqe == nullptr, false)) {
    ring_.Submit();
    sqe = ring_.TryGetSqe();
    if (sqe != nullptr) {
      break;
    }

    ProcessEvents();
    sqe = ring_.TryGetSqe();
  }
  return sqe;
}

}  // namespace blockcache
}  // namespace dingofs
