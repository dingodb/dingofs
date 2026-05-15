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

#include "cache/blockcache/io_uring.h"

#include <butil/memory/scope_guard.h>
#include <glog/logging.h>
#include <liburing.h>
#include <unistd.h>

#include <cstring>

#include "cache/blockcache/aio.h"
#include "cache/common/macro.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

IOUring::IOUring(IOUringOptions options)
    : options_(std::move(options)),
      running_(false),
      io_uring_(),
      fixed_buffers_(std::make_unique<FixedBuffers>(
          std::move(options_.fixed_write_buffers),
          std::move(options_.fixed_read_buffers))),
      epoll_fd_(-1) {}

IOUring::~IOUring() = default;

Status IOUring::Start() {
  if (running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "IOUring already started";
    return Status::OK();
  }

  LOG(INFO) << "IOUring is starting...";

  if (!Supported()) {
    LOG(ERROR) << "Current system kernel not support io_uring";
    return Status::NotSupport("not support io_uring");
  }

  Status status = InitIOUring();
  if (!status.ok()) {
    return status;
  }

  BRPC_SCOPE_EXIT {
    if (!status.ok()) {
      Cleanup();
    }
  };

  status = RegisterBuffers();
  if (!status.ok()) {
    return status;
  }

  status = SetupEpoll();
  if (!status.ok()) {
    return status;
  }

  running_.store(true, std::memory_order_relaxed);
  LOG(INFO) << "Successfully started " << *this;
  return Status::OK();
}

Status IOUring::Shutdown() {
  if (!running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "IOUring already shutdown";
    return Status::OK();
  }

  LOG(INFO) << "IOUring is shutting down...";

  Cleanup();

  running_.store(false, std::memory_order_relaxed);
  LOG(INFO) << "Successfully shutdown IOUring";
  return Status::OK();
}

bool IOUring::Supported() {
  struct io_uring ring;
  int rc = io_uring_queue_init(16, &ring, 0);
  if (rc < 0) {
    LOG(ERROR) << "Fail to init io_uring_queue: " << strerror(-rc);
    return false;
  }

  io_uring_queue_exit(&ring);
  return true;
}

Status IOUring::InitIOUring() {
  int flags = options_.use_sqpoll ? IORING_SETUP_SQPOLL : 0;

  int rc = io_uring_queue_init(options_.entries, &io_uring_, flags);
  if (rc != 0) {
    LOG(ERROR) << "Fail to init io_uring queue: " << strerror(-rc);
    return Status::Internal("init io_uring failed");
  }
  return Status::OK();
}

Status IOUring::RegisterBuffers() {
  auto& buffers = fixed_buffers_->buffers;
  if (buffers.empty()) {
    return Status::OK();
  }

  int rc =
      io_uring_register_buffers(&io_uring_, buffers.data(), buffers.size());
  if (rc < 0) {
    LOG(ERROR) << "Fail to register buffers: " << strerror(-rc);
    return Status::Internal("register buffers failed");
  }
  return Status::OK();
}

Status IOUring::SetupEpoll() {
  epoll_fd_ = epoll_create1(0);
  if (epoll_fd_ < 0) {
    PLOG(ERROR) << "Fail to create epoll";
    return Status::Internal("create epoll failed");
  }

  struct epoll_event ev;
  ev.events = EPOLLIN;
  ev.data.fd = io_uring_.ring_fd;
  int rc = epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, io_uring_.ring_fd, &ev);
  if (rc != 0) {
    PLOG(ERROR) << "Fail to add epoll event";
    return Status::Internal("add epoll event failed");
  }
  return Status::OK();
}

void IOUring::Cleanup() {
  if (!fixed_buffers_->buffers.empty()) {
    io_uring_unregister_buffers(&io_uring_);
  }

  io_uring_queue_exit(&io_uring_);

  if (epoll_fd_ >= 0) {
    close(epoll_fd_);
    epoll_fd_ = -1;
  }
}

void IOUring::PrepWrite(io_uring_sqe* sqe, Aio* aio) const {
  const auto& attr = aio->Attr();
  if (attr.buf_index >= 0) {
    int idx = fixed_buffers_->GetIndex(false, attr.buf_index);
    CHECK_GE(idx, 0) << "Invalid write buffer index " << attr.buf_index;
    io_uring_prep_write_fixed(sqe, attr.fd, attr.buffer, attr.length,
                              attr.offset, idx);
  } else {
    io_uring_prep_write(sqe, attr.fd, attr.buffer, attr.length, attr.offset);
  }
}

void IOUring::PrepRead(io_uring_sqe* sqe, Aio* aio) const {
  const auto& attr = aio->Attr();
  if (attr.buf_index >= 0) {
    int idx = fixed_buffers_->GetIndex(true, attr.buf_index);
    CHECK_GE(idx, 0) << "Invalid read buffer index " << attr.buf_index;
    io_uring_prep_read_fixed(sqe, attr.fd, attr.buffer, attr.length,
                             attr.offset, idx);
  } else {
    io_uring_prep_read(sqe, attr.fd, attr.buffer, attr.length, attr.offset);
  }
}

Status IOUring::PrepareIO(Aio* aio) {
  DCHECK_RUNNING("IOUring");

  struct io_uring_sqe* sqe = io_uring_get_sqe(&io_uring_);
  CHECK_NOTNULL(sqe);

  if (aio->Attr().for_read) {
    PrepRead(sqe, aio);
  } else {
    PrepWrite(sqe, aio);
  }

  io_uring_sqe_set_data(sqe, (void*)aio);
  return Status::OK();
}

Status IOUring::SubmitIO() {
  DCHECK_RUNNING("IOUring");

  int n = io_uring_submit(&io_uring_);
  if (n < 0) {
    LOG(ERROR) << "Fail to submit io: " << strerror(-n);
    return Status::Internal("submit io failed");
  }
  return Status::OK();
}

int IOUring::WaitIO(uint64_t timeout_ms, Aio* completed_aios[]) {
  DCHECK_RUNNING("IOUring");

  struct epoll_event ev;
  int rc = TEMP_FAILURE_RETRY(epoll_wait(epoll_fd_, &ev, 1, timeout_ms));
  if (rc < 0) {
    PLOG(ERROR) << "Fail to wait epoll";
    return 0;
  } else if (rc == 0) {
    return 0;
  }

  int nr = 0;
  unsigned head;
  struct io_uring_cqe* cqe;
  io_uring_for_each_cqe(&io_uring_, head, cqe) {
    auto* aio = static_cast<Aio*>(io_uring_cqe_get_data(cqe));
    OnComplete(aio, cqe->res);
    completed_aios[nr++] = aio;
  }

  io_uring_cq_advance(&io_uring_, nr);
  return nr;
}

void IOUring::OnComplete(Aio* aio, int result) {
  const auto& attr = aio->Attr();
  Status status;
  if (result < 0) {
    status = Status::IoError(strerror(-result));
    LOG(ERROR) << *aio << " failed: " << strerror(-result);
  } else if (result != static_cast<int>(attr.length)) {
    status = Status::IoError("short io");
    LOG(ERROR) << *aio << " incomplete: expected " << attr.length
               << " bytes, got " << result;
  } else {
    status = Status::OK();
    VLOG(3) << *aio << " completed successfully";
  }
  aio->Result().status = std::move(status);
}

std::ostream& operator<<(std::ostream& os, const IOUring& r) {
  return os << "IOUring{entries=" << r.options_.entries
            << " sqpoll=" << r.options_.use_sqpoll
            << " fixed_buffers=" << r.fixed_buffers_->buffers.size() << "}";
}

}  // namespace cache
}  // namespace dingofs
