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

/*
 * Project: DingoFS
 * Created Date: 2026-04-22
 * Author: Jingli Chen (Wine93)
 */

#include "cache/infiniband/event.h"

#include <butil/third_party/murmurhash3/murmurhash3.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <mutex>

#include "common/status.h"

namespace dingofs {
namespace cache {
namespace infiniband {

DEFINE_int32(rdma_event_dispatcher_num, 1, "Number of rdma event dispatcher");

static EventDispatcher* g_dispatchers = nullptr;
static std::once_flag g_init_once;

static void ShutdownGlobalDispatchers() {
  for (int i = 0; i < FLAGS_rdma_event_dispatcher_num; i++) {
    g_dispatchers[i].Shutdown();
  }
}

void InitializeGlobalDispatchers() {
  g_dispatchers = new EventDispatcher[FLAGS_rdma_event_dispatcher_num];
  for (int i = 0; i < FLAGS_rdma_event_dispatcher_num; i++) {
    CHECK(g_dispatchers[i].Start().ok());
  }

  CHECK_EQ(0, atexit(ShutdownGlobalDispatchers));
}

EventDispatcher& GetGlobalEventDispatcher(int fd) {
  std::call_once(g_init_once, InitializeGlobalDispatchers);
  if (FLAGS_rdma_event_dispatcher_num == 1) {
    return g_dispatchers[0];
  }
  return g_dispatchers[butil::fmix64(static_cast<uint64_t>(fd)) %
                       FLAGS_rdma_event_dispatcher_num];
}

EventDispatcher::EventDispatcher() : epoll_fd_(-1), running_(false) {}

EventDispatcher::~EventDispatcher() {
  if (running_.load(std::memory_order_acquire)) {
    Shutdown();
  }
}

Status EventDispatcher::Start() {
  if (running_.load(std::memory_order_acquire)) {
    LOG(WARNING) << "EventDispatcher already started";
    return Status::OK();
  }

  epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
  if (epoll_fd_ < 0) {
    PLOG(ERROR) << "Fail to create epoll fd";
    return Status::Internal("create epoll fd failed");
  }

  running_.store(true, std::memory_order_release);
  worker_thread_ = std::thread(&EventDispatcher::EventWorker, this);
  return Status::OK();
}

Status EventDispatcher::Shutdown() {
  if (!running_.exchange(false, std::memory_order_acq_rel)) {
    LOG(WARNING) << "EventDispatcher already shutdown";
    return Status::OK();
  }

  NotifyAndWaitWorkerStop();

  if (epoll_fd_ >= 0) {
    ::close(epoll_fd_);
    epoll_fd_ = -1;
  }
  return Status::OK();
}

void EventDispatcher::NotifyAndWaitWorkerStop() {
  int notify_fd = eventfd(0, EFD_CLOEXEC);
  CHECK_GE(notify_fd, 0);

  uint64_t one = 1;
  ssize_t nwritten = ::write(notify_fd, &one, sizeof(one));
  CHECK_EQ(nwritten, sizeof(one));
  CHECK(AddEvent(notify_fd, EventType::kReadEvent, nullptr).ok());

  if (worker_thread_.joinable()) {
    worker_thread_.join();
  }

  if (notify_fd >= 0) {
    ::close(notify_fd);
  }
}

Status EventDispatcher::AddEvent(int fd, EventType type,
                                 std::shared_ptr<EventHandler> handler) {
  {
    std::lock_guard<std::mutex> guard(mutex_);
    handlers_[fd] = std::move(handler);
  }

  epoll_event ee{};
  // Key by fd, not by handler pointer: the worker resolves the (strong) handler
  // from handlers_ under the lock, so a torn-down handler is never dereferenced.
  ee.data.fd = fd;
  ee.events = EPOLLET | (type == EventType::kReadEvent ? EPOLLIN : EPOLLOUT);
  int rc = epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ee);
  if (rc != 0) {
    PLOG(ERROR) << "Fail to add event to epoll, fd=" << fd;
    std::lock_guard<std::mutex> guard(mutex_);
    handlers_.erase(fd);
    return Status::Internal("add event failed");
  }
  return Status::OK();
}

Status EventDispatcher::DelEvent(int fd) {
  // Stop new events first, then drop the dispatcher's reference. A handler the
  // worker already picked up stays alive via the worker's local strong ref
  // until HandleEvent() returns.
  int rc = epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
  {
    std::lock_guard<std::mutex> guard(mutex_);
    handlers_.erase(fd);
  }
  if (rc != 0) {
    PLOG(ERROR) << "Fail to del event from epoll, fd=" << fd;
    return Status::Internal("del event failed");
  }
  return Status::OK();
}

void EventDispatcher::EventWorker() {
  epoll_event events[kMaxEvents];
  while (running_.load(std::memory_order_acquire)) {
    int n = TEMP_FAILURE_RETRY(epoll_wait(epoll_fd_, events, kMaxEvents, -1));
    if (n < 0) {
      PLOG(ERROR) << "Fail to wait epoll events";
      break;
    }

    for (int i = 0; i < n; ++i) {
      // Take a strong reference so the handler cannot be freed by a concurrent
      // teardown (DelEvent + owner drop) while HandleEvent() runs.
      std::shared_ptr<EventHandler> handler;
      {
        std::lock_guard<std::mutex> guard(mutex_);
        auto it = handlers_.find(events[i].data.fd);
        if (it != handlers_.end()) {
          handler = it->second;
        }
      }
      if (handler) {
        handler->HandleEvent();
      }
    }
  }
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
