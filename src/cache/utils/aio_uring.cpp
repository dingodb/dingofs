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

#include "cache/utils/aio_uring.h"

#include <absl/strings/str_format.h>
#include <glog/logging.h>
#include <liburing.h>
#include <sys/epoll.h>

#include <cstdint>
#include <cstring>

#include "cache/utils/aio.h"
#include "cache/utils/utils.h"

namespace dingofs {
namespace cache {
namespace utils {

LinuxIoUring::LinuxIoUring()
    : running_(false), io_depth_(0), io_uring_(), epoll_fd_(-1) {}

bool LinuxIoUring::Supported() {
  struct io_uring ring;
  int rc = io_uring_queue_init(16, &ring, 0);
  if (rc < 0) {
    LOG(ERROR) << Errorf("io_uring_queue_init(16)");
    return false;
  }
  io_uring_queue_exit(&ring);
  return true;
}

Status LinuxIoUring::Init(uint32_t io_depth) {
  if (running_.exchange(true)) {  // already running
    return Status::OK();
  }

  if (!Supported()) {
    LOG(WARNING) << "Current system kernel not support io_uring.";
    return Status::NotSupport("not support io_uring");
  }

  unsigned flags = IORING_SETUP_SQPOLL;  // TODO: flags
  int rc = io_uring_queue_init(io_depth, &io_uring_, flags);
  if (rc < 0) {
    LOG(ERROR) << Errorf("io_uring_queue_init(%d)", io_depth);
    return Status::Internal("io_uring_queue_init() failed");
  }

  epoll_fd_ = epoll_create1(0);
  if (epoll_fd_ < 0) {
    LOG(ERROR) << Errorf("epoll_create1(0)");
    return Status::Internal("epoll_create1() failed");
  }

  struct epoll_event ev;
  ev.events = EPOLLIN;
  rc = epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, io_uring_.ring_fd, &ev);
  if (rc != 0) {
    LOG(ERROR) << Errorf("epoll_ctl(%d, EPOLL_CTL_ADD, %d)", epoll_fd_,
                         io_uring_.ring_fd);
    return Status::Internal("epoll_ctl() failed");
  }

  io_depth_ = io_depth;
  return Status::OK();
}

void LinuxIoUring::Shutdown() { io_uring_queue_exit(&io_uring_); }

Status LinuxIoUring::PrepareIo(Aio* aio) {
  struct io_uring_sqe* sqe = io_uring_get_sqe(&io_uring_);
  CHECK_NOTNULL(sqe);

  if (aio->aio_type == AioType::kWrite) {
    io_uring_prep_write(sqe, aio->fd, aio->buffer, aio->length, aio->offset);
  } else if (aio->aio_type == AioType::kRead) {
    io_uring_prep_read(sqe, aio->fd, aio->buffer, aio->length, aio->offset);
  } else {
    CHECK(false) << "Unknown aio type.";  // never happend
  }

  io_uring_sqe_set_data(sqe, (void*)aio);
  return Status::OK();
}

Status LinuxIoUring::SubmitIo() {
  int rc = io_uring_submit(&io_uring_);
  if (rc != 0) {
    LOG(ERROR) << Errorf("io_uring_submit()");
    return Status::Internal("io_uring_submit");
  }
  return Status::OK();
}

Status LinuxIoUring::WaitIo(uint32_t timeout_ms, std::vector<Aio*>* aios) {
  struct epoll_event ev;
  int n = epoll_wait(epoll_fd_, &ev, io_depth_, timeout_ms);
  if (n < 0) {
    LOG(ERROR) << Errorf("epoll_wait(%d,%d,%d)", epoll_fd_, io_depth_,
                         timeout_ms);
    return Status::Internal("epoll_wait() failed");
  } else if (n == 0) {
    return Status::OK();
  }

  // n > 0: any aio completed
  unsigned head;
  struct io_uring_cqe* cqe;
  io_uring_for_each_cqe(&io_uring_, head, cqe) {
    struct Aio* aio = (struct Aio*)(::io_uring_cqe_get_data(cqe));
    aio->retcode = cqe->res;
    aios->emplace_back(aio);
  }

  io_uring_cq_advance(&io_uring_, aios->size());
  return Status::OK();
}

void LinuxIoUring::PostIo(Aio* /*aio*/) {}

}  // namespace utils
}  // namespace cache
}  // namespace dingofs
