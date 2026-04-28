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

#ifndef DINGOFS_SRC_CACHE_INFINIBAND_EVENT_H_
#define DINGOFS_SRC_CACHE_INFINIBAND_EVENT_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <thread>

#include "common/status.h"

namespace dingofs {
namespace cache {
namespace infiniband {

class EventHandler {
 public:
  virtual ~EventHandler() = default;
  virtual void HandleEvent() = 0;
};

enum class EventType : uint8_t {
  kReadEvent = 0,
  kWriteEvent = 1,
};

class EventDispatcher {
 public:
  EventDispatcher();
  ~EventDispatcher();

  Status Start();
  Status Shutdown();

  Status AddEvent(int fd, EventType type, EventHandler* handler) const;
  Status DelEvent(int fd) const;

 private:
  static constexpr const int kMaxEvents = 1024;

  void EventWorker();
  void NotifyAndWaitWorkerStop();

  int epoll_fd_;
  std::atomic<bool> running_;
  std::thread worker_thread_;
};

void InitializeGlobalDispatchers();
EventDispatcher& GetGlobalEventDispatcher(int fd);

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_EVENT_H_
