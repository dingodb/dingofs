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

#include <absl/strings/str_format.h>
#include <absl/strings/str_join.h>
#include <butil/time.h>

#include <string>
#include <vector>

namespace dingofs {
namespace cache {

struct ChildTimer {
  ChildTimer(const std::string& step_name) : step_name(step_name) {}

  void Start() { timer.start(); }
  void Stop() { timer.stop(); }

  std::string step_name;
  butil::Timer timer;
};

std::string TimerMessage(const std::vector<ChildTimer>& child_timers);

class StepTimer {
 public:
  StepTimer() = default;
  virtual ~StepTimer() = default;

  void Start();
  void Stop();

  void NextStep(const std::string& step_name);
  std::string LastStep();

  int64_t UElapsed();
  std::string ToString();

 private:
  void StopPreChildTimer();
  void StartNewChildTimer(const std::string& step_name);

  butil::Timer timer_;
  std::vector<ChildTimer> child_timers_;
};

struct StepTimerGuard {
  StepTimerGuard(StepTimer& timer) : timer(timer) { timer.Start(); }
  ~StepTimerGuard() { timer.Stop(); }

  StepTimer& timer;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_UTILS_PHASE_TIMER_H_
