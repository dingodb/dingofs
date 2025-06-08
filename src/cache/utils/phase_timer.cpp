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

#include "cache/utils/phase_timer.h"

#include <absl/strings/str_format.h>
#include <butil/time.h>

#include <unordered_map>

#include "base/string/string.h"

namespace dingofs {
namespace cache {

using base::string::StrJoin;

static const std::unordered_map<Phase, std::string> kPhases = {
    // block cache
    {Phase::kStageBlock, "stage"},
    {Phase::kRemoveStageBlock, "removestage"},
    {Phase::kCacheBlock, "cache"},
    {Phase::kLoadBlock, "load"},

    // disk cache
    {Phase::kOpenFile, "open"},
    {Phase::kWriteFile, "write"},
    {Phase::kReadFile, "read"},
    {Phase::kLinkFile, "link"},
    {Phase::kRemoveFile, "remove"},
    {Phase::kCacheAdd, "cache_add"},
    {Phase::kEnterUploadQueue, "enqueue"},

    // aio
    {Phase::kWaitThrottle, "throttle"},
    {Phase::kCheckIO, "check"},
    {Phase::kEnterPrepareQueue, "enqueue"},
    {Phase::kPrepareIO, "prepare"},
    {Phase::kSubmitIO, "submit"},
    {Phase::kExecuteIO, "execute"},

    // s3
    {Phase::kS3Put, "s3_put"},
    {Phase::kS3Range, "s3_range"},

    // unknown
    {Phase::kUnknown, "unknown"},
};

std::string StrPhase(Phase phase) {
  auto it = kPhases.find(phase);
  if (it != kPhases.end()) {
    return it->second;
  }
  return "unknown";
}

void PhaseTimer::NextPhase(Phase phase) {
  StopPreTimer();
  StartNewTimer(phase);
}

Phase PhaseTimer::GetPhase() {
  if (timers_.empty()) {
    return Phase::kUnknown;
  }
  return timers_.back().phase;
}

std::string PhaseTimer::ToString() {
  StopPreTimer();

  std::vector<std::string> description;
  for (const auto& timer : timers_) {
    auto elapsed =
        absl::StrFormat("%s:%.6f", StrPhase(timer.phase), timer.elapsed_s);
    description.emplace_back(elapsed);
  }

  if (description.empty()) {
    return "";
  }
  return " (" + StrJoin(description, ",") + ")";
}

void PhaseTimer::StopPreTimer() {
  if (!timers_.empty()) {
    timers_.back().Stop();
  }
}

void PhaseTimer::StartNewTimer(Phase phase) {
  auto timer = Timer(phase);
  timer.Start();
  timers_.emplace_back(timer);
}

}  // namespace cache
}  // namespace dingofs
