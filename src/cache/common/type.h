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
 * Created Date: 2025-05-13
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_COMMON_TYPE_H_
#define DINGOFS_SRC_CACHE_COMMON_TYPE_H_

#include <bthread/condition_variable.h>
#include <bthread/countdown_event.h>
#include <bthread/mutex.h>

#include <memory>

#include "utils/concurrent/rw_lock.h"
#include "utils/concurrent/task_thread_pool.h"

namespace dingofs {
namespace cache {

using BthreadMutex = bthread::Mutex;
using BthreadConditionVariable = bthread::ConditionVariable;
using BthreadCountdownEvent = bthread::CountdownEvent;
using BthreadCountdownEventSPtr = std::shared_ptr<BthreadCountdownEvent>;
using BthreadRWLock = dingofs::utils::BthreadRWLock;
using ReadLockGuard = dingofs::utils::ReadLockGuard;
using WriteLockGuard = dingofs::utils::WriteLockGuard;
using TaskThreadPool =
    dingofs::utils::TaskThreadPool<BthreadMutex, BthreadConditionVariable>;
using TaskThreadPoolUPtr = std::unique_ptr<TaskThreadPool>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_COMMON_TYPE_H_
