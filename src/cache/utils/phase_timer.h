/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-08-19
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_UTILS_PHASE_TIMER_H_
#define DINGOFS_SRC_CACHE_UTILS_PHASE_TIMER_H_

#include <butil/time.h>

#include <string>
#include <vector>

namespace dingofs {
namespace cache {
namespace utils {

enum class Phase : uint8_t {
  // unknown
  kUnknown = 0,

  // block cache
  kStageBlock = 1,
  kCacheBlock = 2,
  kLoadBlock = 3,
  kReadBlock = 4,

  // s3
  kS3Put = 10,
  kS3Range = 11,

  // disk cache
  kOpenFile = 20,
  kWriteFile = 21,
  kReadFile = 22,
  kLink = 23,
  kCacheAdd = 24,
  kEnqueueUpload = 25,

  // aio
  kQueued = 30,
  kCheckIO = 31,
  kEnqueue = 32,
  kPrepareIO = 33,
  kSubmitIO = 34,
  kExecuteIO = 35,
};

std::string StrPhase(Phase phase);

class PhaseTimer {
  struct Timer {
    Timer(Phase phase) : phase(phase) {}

    void Start() { timer.start(); }

    void Stop() {
      timer.stop();
      s_elapsed = timer.u_elapsed() / 1e6;
    }

    Phase phase;
    butil::Timer timer;
    double s_elapsed;
  };

 public:
  PhaseTimer();

  virtual ~PhaseTimer() = default;

  void NextPhase(Phase phase);

  Phase CurrentPhase();

  std::string ToString();

  int64_t TotalUElapsed();

 private:
  void StopPreTimer();

  void StartNewTimer(Phase phase);

 private:
  butil::Timer g_timer_;
  std::vector<Timer> timers_;
};

}  // namespace utils
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_UTILS_PHASE_TIMER_H_
