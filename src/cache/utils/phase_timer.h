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
  // tier block cache
  kLocalPut = 0,
  kRemotePut = 1,
  kLocalRange = 2,
  kRemoteRange = 3,
  kLocalCache = 4,
  kRemoteCache = 5,
  kLocalPrefetch = 6,
  kRemotePrefetch = 7,

  // block cache
  kStageBlock = 10,
  kRemoveStageBlock = 11,
  kCacheBlock = 12,
  kLoadBlock = 13,

  // disk cache
  kOpenFile = 20,
  kWriteFile = 21,
  kReadFile = 22,
  kLinkFile = 23,
  kRemoveFile = 24,
  kCacheAdd = 25,
  kEnterUploadQueue = 26,

  // aio
  kWaitThrottle = 30,
  kCheckIO = 31,
  kEnterPrepareQueue = 32,
  kPrepareIO = 33,
  kExecuteIO = 34,

  // remote block cache
  kRPCPut = 40,
  kRPCRange = 41,
  kRPCCache = 42,
  kRPCPrefetch = 43,

  // s3
  kS3Put = 50,
  kS3Range = 51,

  // block cache service
  kNodePut = 60,
  kNodeRange = 61,
  kNodeCache = 62,
  kNodePrefetch = 63,
  kSendResponse = 64,

  // unknown
  kUnknown = 100,
};

std::string StrPhase(Phase phase);

class PhaseTimer {
 public:
  PhaseTimer();
  virtual ~PhaseTimer() = default;

  void NextPhase(Phase phase);
  Phase GetPhase();

  int64_t UElapsed();

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
  butil::Timer g_timer_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_UTILS_PHASE_TIMER_H_
