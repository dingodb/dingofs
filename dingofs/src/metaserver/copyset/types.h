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
 * Date: Fri Aug  6 17:10:54 CST 2021
 * Author: wuhanqing
 */

#ifndef DINGOFS_SRC_METASERVER_COPYSET_TYPES_H_
#define DINGOFS_SRC_METASERVER_COPYSET_TYPES_H_

#include <braft/configuration.h>
#include <braft/raft.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <cstdint>

#include "utils/concurrent/rw_lock.h"

namespace dingofs {
namespace metaserver {
namespace copyset {

const char RAFT_DATA_DIR[] = "data";
const char RAFT_META_DIR[] = "raft_meta";
const char RAFT_SNAP_DIR[] = "raft_snapshot";
const char RAFT_LOG_DIR[] = "raft_log";

using GroupId = braft::GroupId;
using GroupNid = uint64_t;

using Mutex = ::bthread::Mutex;
using CondVar = ::bthread::ConditionVariable;
using RWLock = ::dingofs::utils::BthreadRWLock;

using ReadLockGuard = ::dingofs::utils::ReadLockGuard;
using WriteLockGuard = ::dingofs::utils::WriteLockGuard;

}  // namespace copyset
}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_SRC_METASERVER_COPYSET_TYPES_H_
