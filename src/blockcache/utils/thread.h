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
#include <sched.h>

#include <cstdio>
#include <string_view>
#include <vector>

namespace dingofs {
namespace blockcache {

inline void SetThreadName(std::string_view name) {
  char buffer[16];
  (void)std::snprintf(buffer, sizeof(buffer), "%.*s",
                      static_cast<int>(name.size()), name.data());
  (void)::pthread_setname_np(::pthread_self(), buffer);
}

inline bool BindThreadToCpu(pthread_t thread, int cpu) {
  cpu_set_t mask;
  CPU_ZERO(&mask);
  CPU_SET(cpu, &mask);
  int rc = ::pthread_setaffinity_np(thread, sizeof(mask), &mask);
  if (rc != 0) {
    LOG(WARNING) << "Fail to bind thread to cpu=" << cpu << ": "
                 << std::strerror(rc);
    return false;
  }
  return true;
}

inline std::vector<int> GetThreadCpus(pthread_t thread) {
  cpu_set_t mask;
  CPU_ZERO(&mask);
  (void)::pthread_getaffinity_np(thread, sizeof(mask), &mask);
  std::vector<int> cpus;
  for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
    if (CPU_ISSET(cpu, &mask)) {
      cpus.push_back(cpu);
    }
  }
  return cpus;
}

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_UTILS_THREAD_H_
