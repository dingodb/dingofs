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

#ifndef CURVEFS_SRC_CLIENT_BLOCKCACHE_PHASE_TIMER_H_
#define CURVEFS_SRC_CLIENT_BLOCKCACHE_PHASE_TIMER_H_

#include <butil/time.h>

#include <string>
#include <vector>

namespace curvefs {
namespace client {
namespace blockcache {

enum class Phase {
  // block cache
  STAGE_BLOCK,
  CACHE_BLOCK,
  LOAD_BLOCK,
  READ_BLOCK,

  // s3
  S3_PUT,
  S3_RANGE,

  // disk cache
  OPEN_FILE,
  WRITE_FILE,
  READ_FILE,
  LINK,
  CACHE_ADD,
  ENQUEUE_UPLOAD,
};

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
  PhaseTimer() = default;

  virtual ~PhaseTimer() = default;

  void NextPhase(Phase phase);

  std::string ToString();

 private:
  void StopPreTimer();

  void StartNewTimer(Phase phase);

  std::string StrPhase(Phase phase);

 private:
  std::vector<Timer> timers_;
};

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BLOCKCACHE_PHASE_TIMER_H_
