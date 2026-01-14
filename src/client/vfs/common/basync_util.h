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

#ifndef DINGOFS_CLIENT_VFS_COMMON_BASYNC_UTIL_H_
#define DINGOFS_CLIENT_VFS_COMMON_BASYNC_UTIL_H_

#include <bthread/bthread.h>

#include "common/callback.h"
#include "common/status.h"

namespace dingofs {
namespace client {
namespace vfs {

class BSynchronizer {
 public:
  BSynchronizer() {
    CHECK(bthread_mutex_init(&mutex_, nullptr) == 0)
        << "bthread_mutex_init fail.";
    CHECK(bthread_cond_init(&cond_, nullptr) == 0) << "bthread_cond_init fail.";
  }

  ~BSynchronizer() {
    bthread_cond_destroy(&cond_);
    bthread_mutex_destroy(&mutex_);
  }

  void Wait() {
    bthread_mutex_lock(&mutex_);
    while (!fire_) {
      bthread_cond_wait(&cond_, &mutex_);
    }
    bthread_mutex_unlock(&mutex_);
  }

  StatusCallback AsStatusCallBack(Status& in_staus) {
    return [&](Status s) {
      in_staus = s;
      Fire();
    };
  }

  void Fire() {
    bthread_mutex_lock(&mutex_);
    fire_ = true;
    bthread_cond_signal(&cond_);
    bthread_mutex_unlock(&mutex_);
  }

 private:
  bthread_mutex_t mutex_;
  bthread_cond_t cond_;

  bool fire_{false};
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_COMMON_BASYNC_UTIL_H_