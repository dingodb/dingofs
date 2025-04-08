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

#include "client/blockcache/phase_timer.h"

#include <absl/strings/str_format.h>
#include <butil/time.h>

#include <unordered_map>

#include "base/string/string.h"

namespace dingofs {
namespace client {
namespace blockcache {

using base::string::StrFormat;
using base::string::StrJoin;

std::string StrPhase(Phase phase) {
  static const std::unordered_map<Phase, std::string> kPhases = {
      // unknown
      {Phase::kUnknown, "unknown"},

      // block cache
      {Phase::kStageBlock, "stage_block"},
      {Phase::kCacheBlock, "cache_block"},
      {Phase::kLoadBlock, "load_block"},
      {Phase::kReadBlock, "read_block"},

      // s3
      {Phase::kS3Put, "s3_put"},
      {Phase::kS3Range, "s3_range"},

      // disk cache
      {Phase::kOpenFile, "open"},
      {Phase::kWriteFile, "write"},
      {Phase::kReadFile, "read"},
      {Phase::kLink, "link"},
      {Phase::kCacheAdd, "cache_add"},
      {Phase::kEnqueueUpload, "enqueue"},

      // aio
      {Phase::kQueued, "queued"},
      {Phase::kCheckIo, "check"},
      {Phase::kEnqueue, "enqueue"},
      {Phase::kPrepareIo, "prepare"},
      {Phase::kSubmitIo, "submit"},
      {Phase::kExecuteIo, "execute"},
      {Phase::kMemcpy, "memcpy"},
      {Phase::kRunClosure, "clousre"},
  };

  auto it = kPhases.find(phase);
  if (it != kPhases.end()) {
    return it->second;
  }
  return "unknown";
}

PhaseTimer::PhaseTimer() { g_timer_.start(); }

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

void PhaseTimer::NextPhase(Phase phase) {
  StopPreTimer();
  StartNewTimer(phase);
}

Phase PhaseTimer::CurrentPhase() {
  if (timers_.empty()) {
    return Phase::kUnknown;
  }
  return timers_.back().phase;
}

std::string PhaseTimer::ToString() {
  StopPreTimer();
  std::vector<std::string> out;
  for (const auto& timer : timers_) {
    auto elapsed = StrFormat("%s:%.6f", StrPhase(timer.phase), timer.s_elapsed);
    out.emplace_back(elapsed);
  }

  if (out.empty()) {
    return "";
  }
  return " (" + StrJoin(out, ",") + ")";
}

int64_t PhaseTimer::TotalUElapsed() {
  g_timer_.stop();
  return g_timer_.u_elapsed();
}

}  // namespace blockcache
}  // namespace client
}  // namespace dingofs
