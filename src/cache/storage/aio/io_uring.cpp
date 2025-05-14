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

#include "cache/storage/aio/io_uring.h"

#include <absl/strings/str_format.h>
#include <bits/types/struct_iovec.h>
#include <butil/iobuf.h>
#include <glog/logging.h>
#include <liburing.h>
#include <sys/epoll.h>

#include <cstdint>
#include <cstring>

#include "cache/storage/aio/aio.h"
#include "cache/utils/helper.h"

namespace dingofs {
namespace cache {
namespace storage {

using dingofs::cache::utils::Errorf;

LinuxIOUring::LinuxIOUring(uint32_t io_depth)
    : running_(false), io_depth_(io_depth), io_uring_(), epoll_fd_(-1) {}

uint32_t LinuxIOUring::GetIODepth() { return io_depth_; }

bool LinuxIOUring::Supported() {
  struct io_uring ring;
  int rc = io_uring_queue_init(16, &ring, 0);
  if (rc < 0) {
    LOG(ERROR) << Errorf("io_uring_queue_init(16)");
    return false;
  }
  io_uring_queue_exit(&ring);
  return true;
}

Status LinuxIOUring::Init() {
  if (running_.exchange(true)) {  // already running
    return Status::OK();
  }

  if (!Supported()) {
    LOG(ERROR) << "Current system kernel not support io_uring.";
    return Status::NotSupport("not support io_uring");
  }

  unsigned flags = IORING_SETUP_SQPOLL;  // TODO(Wine93): flags
  int rc = io_uring_queue_init(io_depth_, &io_uring_, flags);
  if (rc < 0) {
    LOG(ERROR) << Errorf("io_uring_queue_init(%d)", io_depth_);
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
  return Status::OK();
}

Status LinuxIOUring::Shutdown() {
  if (running_.exchange(false)) {
    io_uring_queue_exit(&io_uring_);
  }
  return Status::OK();
}

void LinuxIOUring::PrepWrite(io_uring_sqe* sqe, AioClosure* aio) {
  auto bufvec = aio->Buffer()->Buffers();
  iovec* iovecs = new iovec[bufvec.size()];
  for (int i = 0; i < bufvec.size(); i++) {
    iovecs[i].iov_base = bufvec[i].data;
    iovecs[i].iov_len = bufvec[i].size;
  }
  aio->SetCtx(iovecs);
  io_uring_prep_writev(sqe, aio->Fd(), iovecs, aio->Length(), aio->Offset());
}

void LinuxIOUring::PrepRead(io_uring_sqe* sqe, AioClosure* aio) {
  char* data = new char[aio->Length()];
  butil::IOBuf iobuf;
  iobuf.append_user_data(data, aio->Length(),
                         [](void* data) { delete[] static_cast<char*>(data); });
  io_uring_prep_read(sqe, aio->Fd(), data, aio->Length(), aio->Offset());
}

Status LinuxIOUring::PrepareIO(AioClosure* aio) {
  struct io_uring_sqe* sqe = io_uring_get_sqe(&io_uring_);
  CHECK_NOTNULL(sqe);

  if (aio->IsWrite()) {
    PrepWrite(sqe, aio);
  } else if (aio->IsRead()) {
    PrepRead(sqe, aio);
  } else {
    CHECK(false) << "Unknown aio type.";  // never happend
  }

  io_uring_sqe_set_data(sqe, (void*)aio);
  return Status::OK();
}

Status LinuxIOUring::SubmitIO() {
  int rc = io_uring_submit(&io_uring_);
  if (rc != 0) {
    LOG(ERROR) << Errorf("io_uring_submit()");
    return Status::Internal("io_uring_submit");
  }
  return Status::OK();
}

Status LinuxIOUring::WaitIO(uint32_t timeout_ms, Aios* aios) {
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
    auto* aio = reinterpret_cast<AioClosure*>(::io_uring_cqe_get_data(cqe));
    OnCompleted(aio, cqe->res);
    aios->emplace_back(aio);
  }

  io_uring_cq_advance(&io_uring_, aios->size());
  return Status::OK();
}

void LinuxIOUring::OnCompleted(AioClosure* aio, int retcode) {
  auto status = (retcode == 0) ? Status::OK() : Status::IoError("aio failed");
  aio->status() = status;

  void* ctx = aio->GetCtx();
  if (ctx) {
    iovec* iovecs = static_cast<iovec*>(ctx);
    delete[] iovecs;
  }
}

}  // namespace storage
}  // namespace cache
}  // namespace dingofs
