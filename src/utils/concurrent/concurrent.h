/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: dingo
 * File Created: Monday, 25th March 2019 11:47:46 am
 * Author: tongguangxun
 */

#ifndef SRC_COMMON_CONCURRENT_CONCURRENT_H_
#define SRC_COMMON_CONCURRENT_CONCURRENT_H_

#include <atomic>
#include <condition_variable>  // NOLINT
#include <thread>              // NOLINT

#include "utils/concurrent/count_down_event.h"
#include "utils/concurrent/rw_lock.h"
#include "utils/concurrent/task_queue.h"
#include "utils/concurrent/task_thread_pool.h"

namespace dingofs {
namespace utils {

// dingo公共组件命名空间替换
template <class T>
using Atomic = std::atomic<T>;
using Mutex = std::mutex;
using Thread = std::thread;
using LockGuard = std::lock_guard<Mutex>;
using UniqueLock = std::unique_lock<Mutex>;
using ConditionVariable = std::condition_variable;

// dingo内部定义的锁组件
using RWLock = BthreadRWLock;
using ReadLockGuard = ReadLockGuard;
using WriteLockGuard = WriteLockGuard;

// dingo内部定义的线程组件
using TaskQueue = TaskQueue;

}  // namespace utils
}  // namespace dingofs
#endif  // SRC_COMMON_CONCURRENT_CONCURRENT_H_
