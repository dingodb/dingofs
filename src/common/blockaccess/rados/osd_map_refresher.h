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

#ifndef DINGOFS_COMMON_BLOCK_ACCESS_RADOS_OSD_MAP_REFRESHER_H_
#define DINGOFS_COMMON_BLOCK_ACCESS_RADOS_OSD_MAP_REFRESHER_H_

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <random>
#include <thread>

namespace dingofs {
namespace blockaccess {

class OsdMapRefresher {
 public:
  struct Options {
    bool enabled{true};
    std::chrono::milliseconds interval{std::chrono::seconds(60)};
    uint32_t jitter_pct{20};
  };

  using RefreshFn = std::function<int()>;

  OsdMapRefresher(Options options, RefreshFn refresh_fn);
  ~OsdMapRefresher();

  OsdMapRefresher(const OsdMapRefresher&) = delete;
  OsdMapRefresher& operator=(const OsdMapRefresher&) = delete;

  bool Start();
  void Stop();

 private:
  void Run();
  std::chrono::milliseconds NextInterval();

  const Options options_;
  const RefreshFn refresh_fn_;

  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::thread thread_;
  std::mt19937_64 random_;

  bool started_{false};
  bool stopping_{false};
};

}  // namespace blockaccess
}  // namespace dingofs

#endif  // DINGOFS_COMMON_BLOCK_ACCESS_RADOS_OSD_MAP_REFRESHER_H_
