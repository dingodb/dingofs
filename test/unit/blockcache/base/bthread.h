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
 * Created Date: 2026-10-11
 * Author: AI
 */

#ifndef DINGOFS_TEST_UNIT_BLOCKCACHE_BASE_BTHREAD_H_
#define DINGOFS_TEST_UNIT_BLOCKCACHE_BASE_BTHREAD_H_

#include <bthread/bthread.h>
#include <bthread/types.h>
#include <gflags/gflags.h>

#include <cerrno>
#include <string>

namespace dingofs {
namespace blockcache {

inline void SyncBthreadConcurrency() {
  const int workers = bthread_getconcurrency_by_tag(BTHREAD_TAG_DEFAULT);
  if (workers == EPERM) {
    return;
  }
  const std::string value = std::to_string(workers);
  gflags::SetCommandLineOption("bthread_concurrency", value.c_str());
}

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_TEST_UNIT_BLOCKCACHE_BASE_BTHREAD_H_
