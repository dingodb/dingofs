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
 * Created Date: 2025-02-19
 * Author: Jingli Chen (Wine93)
 */

#include "cache/common/aio_queue.h"

#include <glog/logging.h>

#include <thread>

namespace dingofs {
namespace cache {
namespace common {

/*
bool AioQueue::Supported() {
  struct io_uring ring;
  int rc = io_uring_queue_init(16, &ring, 0);
  if (rc < 0) {
    return false;
  }
  io_uring_queue_exit(&ring);
  return true;
}
*/

AioQueueImpl::AioQueueImpl()
    : running_(false), io_uring_(), epoll_fd_(-1), submit_queue_id_({0}) {}

bool AioQueueImpl::Init(uint32_t io_depth) {
  if (running_.exchange(true)) {  // already running
    return true;
  }

  unsigned flags = 0;
  flags |= IORING_SETUP_SQPOLL;  // FIXME
  int rc = io_uring_queue_init(io_depth, &io_uring_, flags);
  if (rc < 0) {
    LOG(ERROR) << "io_uring_queue_init() failed, rc=" << rc;
    return false;
  }

  epoll_fd_ = epoll_create1(0);
  if (epoll_fd_ < 0) {
    LOG(ERROR) << "epoll_create1() failed, rc=" << epoll_fd_;
    return false;
  }

  struct epoll_event ev;
  ev.events = EPOLLIN;
  rc = epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, io_uring_.ring_fd, &ev);
  if (rc != 0) {
    LOG(ERROR) << "epoll_ctl() failed, rc=" << rc;
    return false;
  }

  bthread::ExecutionQueueOptions queue_options;
  queue_options.bthread_attr = BTHREAD_ATTR_NORMAL;
  queue_options.use_pthread = true;
  rc = bthread::execution_queue_start(&submit_queue_id_, &queue_options,
                                      BatchSubmit, this);
  if (rc != 0) {
    LOG(ERROR) << "execution_queue_start() failed, rc=" << rc;
    return false;
  }
  thread_ = std::thread(&AioQueueImpl::AioWait, this);
  return true;
}

bool AioQueueImpl::Shutdown() {
  if (running_.exchange(false)) {
    thread_.join();
    bthread::execution_queue_stop(submit_queue_id_);
    return bthread::execution_queue_join(submit_queue_id_) == 0;
  }
  return true;
}

void AioQueueImpl::Submit(const std::vector<AioContext*>& iovec) {
  // CHECK_EQ(0, bthread::execution_queue_execute(submit_queue_id_, iovec));
  for (auto* aio : iovec) {
    PrepareSqe(aio);
  }
  io_uring_submit(&io_uring_);
}

int AioQueueImpl::BatchSubmit(
    void* meta, bthread::TaskIterator<std::vector<AioContext*>>& iter) {
  AioQueueImpl* queue = static_cast<AioQueueImpl*>(meta);
  for (; iter; iter++) {
    auto& iovec = *iter;
    for (auto* aio : iovec) {
      queue->PrepareSqe(aio);
    }
  }
  io_uring_submit(&queue->io_uring_);
  return 0;
}

void AioQueueImpl::PrepareSqe(const AioContext* aio) {
  struct io_uring_sqe* sqe = io_uring_get_sqe(&io_uring_);
  if (nullptr == sqe) {
    CHECK(false) << "submit queue is full.";  // FIXME
  } else if (aio->type == AioType::kWrite) {
    io_uring_prep_write(sqe, aio->fd, aio->buffer, aio->length, aio->offset);
  } else if (aio->type == AioType::kRead) {
    io_uring_prep_read(sqe, aio->fd, aio->buffer, aio->length, aio->offset);
  } else {
    CHECK(false);  // never happend
  }
  io_uring_sqe_set_data(sqe, (void*)aio);
}

void AioQueueImpl::AioWait() {
  struct epoll_event ev;
  int batch_size = 128;
  std::vector<AioContext*> iovec;  // Aio context vector
  std::vector<int> rcv;            // return code vector

  while (running_.load(std::memory_order_relaxed)) {
    int count = GetCompleted(batch_size, &iovec, &rcv);
    if (count > 0) {
      RunCallback(iovec, rcv);
      continue;
    }

    int n = epoll_wait(epoll_fd_, &ev, 1, 100 /* timeout ms*/);
    if (n >= 0) {
      continue;
    } else {
      CHECK(false);
    }
  }
}

int AioQueueImpl::GetCompleted(int batch_size, std::vector<AioContext*>* iovec,
                               std::vector<int>* rcv) {
  struct io_uring* ring = &io_uring_;
  unsigned head;
  struct io_uring_cqe* cqe;
  iovec->clear();
  rcv->clear();

  io_uring_for_each_cqe(ring, head, cqe) {
    struct AioContext* aio = (struct AioContext*)(::io_uring_cqe_get_data(cqe));
    iovec->push_back(aio);
    rcv->push_back(cqe->res);
    if (iovec->size() == batch_size) {
      break;
    }
  }

  io_uring_cq_advance(ring, iovec->size());
  return iovec->size();
}

void AioQueueImpl::RunCallback(const std::vector<AioContext*>& iovec,
                               const std::vector<int>& rcv) {
  for (int i = 0; i < iovec.size(); i++) {
    iovec[i]->aio_cb(iovec[i], rcv[i]);
  }
}

}  // namespace common
}  // namespace cache
}  // namespace dingofs
