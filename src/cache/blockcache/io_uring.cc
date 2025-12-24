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

#include <absl/strings/str_format.h>
#include <butil/memory/aligned_memory.h>
#include <glog/logging.h>
#include <liburing.h>

#include <atomic>
#include <cstdint>
#include <cstring>

#include "cache/blockcache/aio.h"
#include "cache/common/macro.h"
#include "common/io_buffer.h"
#include "common/options/cache.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

IOUring::IOUring(const std::vector<iovec>& fixed_write_buffers,
                 const std::vector<iovec>& fixed_read_buffers)
    : running_(false), io_uring_(), epoll_fd_(-1) {
  // NOTE: only call register_buffers once, so we need to merge the buffers
  fixed_buffers_.reserve(fixed_write_buffers.size() +
                         fixed_read_buffers.size());
  fixed_buffers_.insert(fixed_buffers_.end(), fixed_write_buffers.begin(),
                        fixed_write_buffers.end());
  fixed_buffers_.insert(fixed_buffers_.end(), fixed_read_buffers.begin(),
                        fixed_read_buffers.end());
  write_buf_index_offset_ = 0;
  read_buf_index_offset_ = fixed_write_buffers.size();
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

Status IOUring::Start() {
  if (running_.load(std::memory_order_acquire)) {
    LOG(WARNING) << "IOUring already started";
    return Status::OK();
  }

  LOG(INFO) << "IOUring is starting...";

  if (!Supported()) {
    LOG(ERROR) << "Current system kernel not support io_uring";
    return Status::NotSupport("not support io_uring");
  }

  int flags = IORING_SETUP_SQPOLL;
  int rc = io_uring_queue_init(FLAGS_iodepth, &io_uring_, flags);
  if (rc != 0) {
    LOG(ERROR) << "Fail to init io_uring queue: " << strerror(-rc);
    return Status::Internal("init io_uring failed");
  }

  rc = io_uring_register_buffers(&io_uring_, fixed_buffers_.data(),
                                 fixed_buffers_.size());
  if (rc < 0) {
    LOG(ERROR) << "Fail to register buffers: " << strerror(-rc);
    return Status::Internal("register buffers failed");
  }

  epoll_fd_ = epoll_create1(0);
  if (epoll_fd_ <= 0) {
    PLOG(ERROR) << "Fail to create epoll";
    return Status::Internal("create epoll failed");
  }

  struct epoll_event ev;
  ev.events = EPOLLIN;
  rc = epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, io_uring_.ring_fd, &ev);
  if (rc != 0) {
    PLOG(ERROR) << "Fail to add epoll event";
    return Status::Internal("add epoll event failed");
  }

  running_.store(true, std::memory_order_relaxed);
  LOG(INFO) << "IOUring{iodepth=" << FLAGS_iodepth << "} is up";
  return Status::OK();
}

Status IOUring::Shutdown() {
  if (!running_.load(std::memory_order_acq_rel)) {
    LOG(WARNING) << "IOUring already shutdown";
    return Status::OK();
  }

  LOG(INFO) << "IOUring is shutting down...";

  io_uring_queue_exit(&io_uring_);
  io_uring_unregister_buffers(&io_uring_);
  epoll_fd_ = -1;

  running_.store(false, std::memory_order_relaxed);
  LOG(INFO) << "IOUring is down";
  return Status::OK();
}

void IOUring::PrepWrite(io_uring_sqe* sqe, Aio* aio) const {
  CHECK_GE(aio->buf_index, 0);
  io_uring_prep_write_fixed(sqe, aio->fd, aio->buffer, aio->length, aio->offset,
                            aio->buf_index + write_buf_index_offset_);
}

void IOUring::PrepRead(io_uring_sqe* sqe, Aio* aio) const {
  CHECK_GE(aio->buf_index, 0);
  io_uring_prep_read_fixed(sqe, aio->fd, aio->buffer, aio->length, aio->offset,
                           aio->buf_index + read_buf_index_offset_);
}

Status IOUring::PrepareIO(Aio* aio) {
  DCHECK_RUNNING("IOUring");

  struct io_uring_sqe* sqe = io_uring_get_sqe(&io_uring_);
  CHECK_NOTNULL(sqe);

  if (aio->for_read) {
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
  Status status;
  if (result < 0) {
    status = Status::IoError(strerror(-result));
    LOG(ERROR) << "Fail to execute " << aio << ": " << strerror(-result);
  } else if (result != aio->length) {
    status = Status::IoError("unexpected io length");
    LOG(ERROR) << "Fail to execute " << aio << ", want "
               << (aio->for_read ? "read" : "write") << aio->length
               << " bytes but got " << result << " bytes";
  } else {
    status = Status::OK();
  }

  aio->status() = status;
}

}  // namespace cache
}  // namespace dingofs
