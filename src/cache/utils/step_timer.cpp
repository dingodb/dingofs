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

#include "cache/utils/step_timer.h"

#include <absl/strings/str_format.h>
#include <absl/strings/str_join.h>
#include <butil/time.h>

namespace dingofs {
namespace cache {

void StepTimer::Start() { timer_.start(); }

void StepTimer::Stop() {
  timer_.stop();
  for (auto& child_timer : child_timers_) {
    child_timer.Stop();
  }
}

void StepTimer::Reset() {
  Stop();
  child_timers_.clear();
}

int64_t StepTimer::UElapsed() const { return timer_.u_elapsed(); }

void StepTimer::NextStep(const std::string& step_name) {
  StopPreChildTimer();
  StartNewChildTimer(step_name);
}

std::string StepTimer::GetStep() {
  if (child_timers_.empty()) {
    return "";
  }
  return child_timers_.back().step_name;
}

// e.g. write:0.005317,link:0.000094,cache_add:0.000013,enqueue:0.000004
std::string StepTimer::ToString() {
  StopPreChildTimer();

  std::vector<std::string> messages;
  for (const auto& child_timer : child_timers_) {
    auto message = absl::StrFormat("%s:%.6f", child_timer.step_name,
                                   child_timer.timer.u_elapsed() * 1.0 / 1e6);
    messages.emplace_back(message);
  }

  if (messages.empty()) {
    return "";
  }
  return absl::StrJoin(messages, ",");
}

void StepTimer::StopPreChildTimer() {
  if (!child_timers_.empty()) {
    child_timers_.back().Stop();
  }
}

void StepTimer::StartNewChildTimer(const std::string& step_name) {
  auto timer = ChildTimer(step_name);
  timer.Start();
  child_timers_.emplace_back(timer);
}

}  // namespace cache
}  // namespace dingofs
