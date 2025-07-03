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

#ifndef DINGODB_CLIENT_VFS_DATA_ASYNC_UTIL_H_
#define DINGODB_CLIENT_VFS_DATA_ASYNC_UTIL_H_

#include <condition_variable>
#include <mutex>

#include "common/callback.h"
#include "common/status.h"

namespace dingofs {
namespace client {
namespace vfs {

class Synchronizer {
 public:
  Synchronizer() = default;
  ~Synchronizer() = default;

  void Wait() {
    std::unique_lock<std::mutex> lk(mutex_);
    while (!fire_) {
      cond_.wait(lk);
    }
  }

  StatusCallback AsStatusCallBack(Status& in_staus) {
    return [&](Status s) {
      in_staus = s;
      Fire();
    };
  }

  void Fire() {
    std::unique_lock<std::mutex> lk(mutex_);
    fire_ = true;
    cond_.notify_one();
  }

 private:
  std::mutex mutex_;
  std::condition_variable cond_;

  bool fire_{false};
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGODB_CLIENT_VFS_DATA_ASYNC_UTIL_H_