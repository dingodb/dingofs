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

#include "cache/storage/aio/aio.h"
#include "cache/utils/helper.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

LinuxIOUring::LinuxIOUring(uint32_t iodepth)
    : running_(false), iodepth_(iodepth), io_uring_(), epoll_fd_(-1) {}

bool LinuxIOUring::Supported() {
  struct io_uring ring;
  int rc = io_uring_queue_init(16, &ring, 0);
  if (rc < 0) {
    LOG(ERROR) << Helper::Errorf("io_uring_queue_init(16)");
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
  int rc = io_uring_queue_init(iodepth_, &io_uring_, flags);
  if (rc < 0) {
    LOG(ERROR) << Helper::Errorf("io_uring_queue_init(%d)", iodepth_);
    return Status::Internal("io_uring_queue_init() failed");
  }

  epoll_fd_ = epoll_create1(0);
  if (epoll_fd_ < 0) {
    LOG(ERROR) << Helper::Errorf("epoll_create1(0)");
    return Status::Internal("epoll_create1() failed");
  }

  struct epoll_event ev;
  ev.events = EPOLLIN;
  rc = epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, io_uring_.ring_fd, &ev);
  if (rc != 0) {
    LOG(ERROR) << Helper::Errorf("epoll_ctl(%d, EPOLL_CTL_ADD, %d)", epoll_fd_,
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
  auto bufvec = aio->buffer_in.Fetch();
  iovec* iovecs = new iovec[bufvec.size()];
  for (int i = 0; i < bufvec.size(); i++) {
    iovecs[i].iov_base = bufvec[i].iov_base;
    iovecs[i].iov_len = bufvec[i].iov_len;
  }
  aio->ctx = iovecs;  // Store iovec in aio context for cleanup later
  io_uring_prep_writev(sqe, aio->fd, iovecs, bufvec.size(), aio->offset);
}

void LinuxIOUring::PrepRead(io_uring_sqe* sqe, AioClosure* aio) {
  char* data = new char[aio->length];
  butil::IOBuf iobuf;
  iobuf.append_user_data(data, aio->length,
                         [](void* data) { delete[] static_cast<char*>(data); });
  *aio->buffer_out = IOBuffer(iobuf);

  io_uring_prep_read(sqe, aio->fd, data, aio->length, aio->offset);
}

Status LinuxIOUring::PrepareIO(AioClosure* aio) {
  struct io_uring_sqe* sqe = io_uring_get_sqe(&io_uring_);
  CHECK_NOTNULL(sqe);

  if (IsAioWrite(aio)) {
    PrepWrite(sqe, aio);
  } else if (IsAioRead(aio)) {
    PrepRead(sqe, aio);
  } else {  // never happend
    CHECK(false) << "Unknown aio type: " << static_cast<int>(aio->iotype);
  }

  io_uring_sqe_set_data(sqe, (void*)aio);
  return Status::OK();
}

Status LinuxIOUring::SubmitIO() {
  int n = io_uring_submit(&io_uring_);
  if (n < 0) {
    LOG(ERROR) << Helper::Errorf(-n, "io_uring_submit()");
    return Status::Internal("io_uring_submit");
  }
  return Status::OK();
}

Status LinuxIOUring::WaitIO(uint64_t timeout_ms,
                            std::vector<AioClosure*>* aios) {
  aios->clear();

  struct epoll_event ev;
  int n = epoll_wait(epoll_fd_, &ev, iodepth_, timeout_ms);
  if (n < 0) {
    LOG(ERROR) << Helper::Errorf(-n, "epoll_wait(%d,%lu,%lld)", epoll_fd_,
                                 iodepth_, timeout_ms);
    return Status::Internal("epoll_wait() failed");
  } else if (n == 0) {
    return Status::OK();
  }

  LOG(INFO) << n << " aio completed, waiting for io_uring_cq.";

  // n > 0: any aio completed
  unsigned head;
  struct io_uring_cqe* cqe;
  io_uring_for_each_cqe(&io_uring_, head, cqe) {
    auto* aio = reinterpret_cast<AioClosure*>(io_uring_cqe_get_data(cqe));
    OnCompleted(aio, cqe->res);
    aios->emplace_back(aio);
  }

  io_uring_cq_advance(&io_uring_, aios->size());
  return Status::OK();
}

void LinuxIOUring::OnCompleted(AioClosure* aio, int retcode) {
  auto status = Status::OK();
  if (retcode < 0) {
    LOG(ERROR) << Helper::Errorf(-retcode, aio->ToString().c_str());
    status = Status::IoError("aio failed");
  }

  aio->status() = status;
  if (aio->ctx) {  // only for write
    CleanupCtx(static_cast<Context*>(aio->ctx));
  }
}

void LinuxIOUring::CleanupCtx(Context* ctx) {
  // FIXME: double free
  // delete[] ctx->iovecs;
  // delete ctx;
}

uint32_t LinuxIOUring::GetIODepth() const { return iodepth_; }

}  // namespace cache
}  // namespace dingofs
