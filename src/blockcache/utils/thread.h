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

#ifndef DINGOFS_BLOCKCACHE_UTILS_THREAD_H_
#define DINGOFS_BLOCKCACHE_UTILS_THREAD_H_

#include <glog/logging.h>
#include <pthread.h>

#include <cstdio>
#include <string>
#include <string_view>

namespace dingofs {
namespace blockcache {

inline void SetThreadName(std::string_view name) {
  char buffer[16];
  (void)std::snprintf(buffer, sizeof(buffer), "%.*s",
                      static_cast<int>(name.size()), name.data());
  (void)::pthread_setname_np(::pthread_self(), buffer);
}

inline bool PinThreadToCpu(pthread_t thread, int cpu) {
  cpu_set_t mask;
  CPU_ZERO(&mask);
  CPU_SET(cpu, &mask);
  int rc = ::pthread_setaffinity_np(thread, sizeof(mask), &mask);
  if (rc != 0) {
    LOG(WARNING) << "Fail to pin thread to cpu=" << cpu << ": "
                 << std::strerror(rc);
    return false;
  }
  return true;
}

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_UTILS_THREAD_H_
