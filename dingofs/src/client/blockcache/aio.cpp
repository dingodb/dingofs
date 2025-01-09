/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-11-05
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/src/client/blockcache/aio.h"

#include <glog/logging.h>

#include <atomic>
#include <mutex>
#include <thread>

namespace curvefs {
namespace client {
namespace blockcache {

AioQueue::AioQueue() : running_(false), io_uring_() {}

bool AioQueue::Supported() {
  struct io_uring ring;
  int rc = io_uring_queue_init(16, &ring, 0);
  if (rc) {
    return false;
  }
  io_uring_queue_exit(&ring);
  return true;
}

bool AioQueue::Init(uint32_t io_depth) {
  unsigned flags = 0;
  flags |= IORING_SETUP_SQPOLL;
  int rc = io_uring_queue_init(io_depth, &io_uring_.ring, flags);
  if (rc < 0) {
    return false;
  }

  //::start_sq_polling_ops(&io_uring_.ring);

  int epoll_fd = epoll_create1(0);
  if (epoll_fd < 0) {
    return false;
  }
  io_uring_.epoll_fd = epoll_fd;

  struct epoll_event ev;
  ev.events = EPOLLIN;
  rc = epoll_ctl(epoll_fd, EPOLL_CTL_ADD, io_uring_.ring.ring_fd, &ev);
  if (rc < 0) {
    return false;
  }

  thread_id_ = std::thread(&AioQueue::ReapWorker, this);
  running_ = true;
  return true;
}

void AioQueue::Shutdown() {}

void AioQueue::Submit(std::vector<AioContext> contexts) {
  std::unique_lock<std::mutex> lk(io_uring_.sq_mutex);
  for (auto& ctx : contexts) {
    Enqueue(&ctx);
  }
  ::io_uring_submit(&io_uring_.ring);
}

void AioQueue::Enqueue(AioContext* ctx) {
  struct io_uring_sqe* sqe = io_uring_get_sqe(&io_uring_.ring);
  if (nullptr == sqe) {  // FIXME(@Wine93): queue is full
    CHECK(false) << "io_uring_get_sqe() failed";
    return;
  }

  if (ctx->type == AioType::kWrite) {
    ::io_uring_prep_write(sqe, ctx->fd, ctx->buffer, ctx->length, ctx->offset);
    // LOG(ERROR) << "<<< fd: " << ctx->fd << ", offset: " << ctx->offset
    //            << ", length: " << ctx->length;
  } else if (ctx->type == AioType::kRead) {
    ::io_uring_prep_read(sqe, ctx->fd, ctx->buffer, ctx->length, ctx->offset);
  } else {
    CHECK(false);  // never happend
  }

  ::io_uring_sqe_set_data(sqe, ctx);
}

int AioQueue::GetCompleted(int max, std::vector<AioContext>* contexts) {
  struct io_uring* ring = &io_uring_.ring;
  unsigned head;
  struct io_uring_cqe* cqe;
  int num_completed = 0;

  io_uring_for_each_cqe(ring, head, cqe) {
    struct AioContext* ctx = (struct AioContext*)(::io_uring_cqe_get_data(cqe));
    ctx->retval = cqe->res;
    contexts->push_back(*ctx);
    num_completed++;
    if (num_completed == max) {
      break;
    }
  }
  io_uring_cq_advance(ring, num_completed);
  return num_completed;
}

void AioQueue::OnComplete(std::vector<AioContext>& contexts) {
  for (auto& context : contexts) {
    context.aio_cb(&context);
  }
}

void AioQueue::ReapWorker() {
  struct epoll_event ev;
  int epoll_fd = io_uring_.epoll_fd;
  int timeout_ms = 100;
  std::vector<AioContext> contexts;

  while (running_.load(std::memory_order_relaxed)) {
    contexts.clear();
    int num_completed = GetCompleted(128, &contexts);
    if (num_completed > 0) {
      OnComplete(contexts);
      continue;
    }

    // num_completed == 0
    int n = epoll_wait(epoll_fd, &ev, 1, timeout_ms);
    if (n >= 0) {
      continue;
    } else {
      CHECK(false);
    }
  }
}

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs
