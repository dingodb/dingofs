/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: Thu Jul 22 10:45:43 CST 2021
 * Author: wuhanqing
 */

#ifndef DINGOFS_SRC_MDS_COMMON_TYPES_H_
#define DINGOFS_SRC_MDS_COMMON_TYPES_H_

#include <bthread/mutex.h>

#include "utils/concurrent/rw_lock.h"

namespace dingofs {
namespace mds {

using Mutex = ::bthread::Mutex;
using RWLock = ::dingofs::utils::BthreadRWLock;

using ::dingofs::utils::ReadLockGuard;
using ::dingofs::utils::WriteLockGuard;

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_SRC_MDS_COMMON_TYPES_H_
