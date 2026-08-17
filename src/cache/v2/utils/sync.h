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

#ifndef DINGOFS_CACHE_V2_UTILS_SYNC_H_
#define DINGOFS_CACHE_V2_UTILS_SYNC_H_

#include <utility>
#include <vector>

#include "cache/v2/core/reactor/coroutine.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace v2 {

class SingleFlight {
 public:
  template <typename F>
  Future<Status> Do(F run) {
    if (running_) {
      co_return co_await Join();
    }
    running_ = true;
    const Status status = co_await run();
    Finish(status);
    co_return status;
  }

  bool running() const { return running_; }

 private:
  Future<Status> Join() {
    Promise<Status> promise;
    waiters_.push_back(&promise);
    co_return co_await promise.GetFuture();
  }

  void Finish(const Status& status) {
    running_ = false;
    std::vector<Promise<Status>*> waiters = std::move(waiters_);
    waiters_.clear();
    for (Promise<Status>* waiter : waiters) {
      waiter->SetValue(status);
    }
  }

  bool running_ = false;
  std::vector<Promise<Status>*> waiters_;
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_UTILS_SYNC_H_
