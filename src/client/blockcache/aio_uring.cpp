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

#include "client/blockcache/aio_uring.h"

#include <absl/strings/str_format.h>
#include <glog/logging.h>
#include <hf3fs_usrbio.h>
#include <liburing.h>
#include <sys/epoll.h>

#include <cstdint>
#include <cstring>

#include "client/blockcache/aio.h"
#include "client/blockcache/helper.h"

namespace dingofs {
namespace client {
namespace blockcache {

LinuxIoUring::LinuxIoUring()
    : running_(false), io_depth_(0), io_uring_(), epoll_fd_(-1) {}

bool LinuxIoUring::Supported() {
  struct io_uring ring;
  int rc = io_uring_queue_init(16, &ring, 0);
  if (rc < 0) {
    LOG(ERROR) << Helper::Errorf("io_uring_queue_init(16)");
    return false;
  }
  io_uring_queue_exit(&ring);
  return true;
}

bool LinuxIoUring::Init(uint32_t io_depth) {
  if (running_.exchange(true)) {  // already running
    return true;
  }

  if (!Supported()) {
    LOG(WARNING) << "Current system kernel not support io_uring.";
    return false;
  }

  unsigned flags = IORING_SETUP_SQPOLL;  // TODO: flags
  int rc = io_uring_queue_init(io_depth, &io_uring_, flags);
  if (rc < 0) {
    LOG(ERROR) << Helper::Errorf("io_uring_queue_init(%d)", io_depth);
    return false;
  }

  epoll_fd_ = epoll_create1(0);
  if (epoll_fd_ < 0) {
    LOG(ERROR) << Helper::Errorf("epoll_create1(0)");
    return false;
  }

  struct epoll_event ev;
  ev.events = EPOLLIN;
  rc = epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, io_uring_.ring_fd, &ev);
  if (rc != 0) {
    LOG(ERROR) << Helper::Errorf("epoll_ctl(%d, EPOLL_CTL_ADD, %d)", epoll_fd_,
                                 io_uring_.ring_fd);
    return false;
  }

  io_depth_ = io_depth;
  return true;
}

void LinuxIoUring::Shutdown() { /* TODO*/
}

bool LinuxIoUring::PrepareIo(Aio* aio) {
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
  return true;
}

bool LinuxIoUring::SubmitIo() {
  int rc = io_uring_submit(&io_uring_);
  if (rc != 0) {
    LOG(ERROR) << Helper::Errorf("io_uring_submit()");
    return false;
  }
  return true;
}

bool LinuxIoUring::WaitIo(uint32_t timeout_ms, std::vector<Aio*>* aios) {
  struct epoll_event ev;
  int n = epoll_wait(epoll_fd_, &ev, io_depth_, timeout_ms);
  if (n < 0) {
    LOG(ERROR) << Helper::Errorf("epoll_wait(%d,%d,%d)", epoll_fd_, io_depth_,
                                 timeout_ms);
    return false;
  } else if (n == 0) {
    return true;
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
  return true;
}

void LinuxIoUring::PostIo(Aio* /*aio*/) {}

}  // namespace blockcache
}  // namespace client
}  // namespace dingofs
