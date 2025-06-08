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

enum class Phase : uint8_t {
  // block cache
  kStageBlock = 0,
  kRemoveStageBlock = 1,
  kCacheBlock = 2,
  kLoadBlock = 3,

  // disk cache
  kOpenFile = 10,
  kWriteFile = 11,
  kReadFile = 12,
  kLinkFile = 13,
  kRemoveFile = 14,
  kCacheAdd = 15,
  kEnterUploadQueue = 16,

  // aio
  kWaitThrottle = 20,
  kCheckIO = 21,
  kEnterPrepareQueue = 22,
  kPrepareIO = 23,
  kSubmitIO = 24,
  kExecuteIO = 25,

  // s3
  kS3Put = 30,
  kS3Range = 31,

  // unknown
  kUnknown = 100,
};

std::string StrPhase(Phase phase);

class PhaseTimer {
 public:
  PhaseTimer() = default;
  virtual ~PhaseTimer() = default;

  void NextPhase(Phase phase);
  Phase GetPhase();

  std::string ToString();

 private:
  struct Timer {
    Timer(Phase phase) : phase(phase) {}

    void Start() { timer.start(); }

    void Stop() {
      timer.stop();
      elapsed_s = timer.u_elapsed() / 1e6;
    }

    const Phase phase{Phase::kUnknown};
    butil::Timer timer;
    double elapsed_s{0};
  };

  void StopPreTimer();
  void StartNewTimer(Phase phase);

  std::vector<Timer> timers_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_UTILS_PHASE_TIMER_H_
