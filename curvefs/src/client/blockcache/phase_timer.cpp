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

#include "curvefs/src/client/blockcache/phase_timer.h"

#include <butil/time.h>

#include "curvefs/src/base/string/string.h"

namespace curvefs {
namespace client {
namespace blockcache {

using ::curvefs::base::string::StrFormat;
using ::curvefs::base::string::StrJoin;

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

std::string PhaseTimer::StrPhase(Phase phase) {
  static const std::unordered_map<Phase, std::string> phases = {
      // block cache
      {Phase::STAGE_BLOCK, "stage_block"},
      {Phase::CACHE_BLOCK, "cache_block"},
      {Phase::LOAD_BLOCK, "load_block"},
      {Phase::READ_BLOCK, "read_block"},
      // s3
      {Phase::S3_PUT, "s3_put"},
      {Phase::S3_RANGE, "s3_range"},
      // disk cache
      {Phase::OPEN_FILE, "open"},
      {Phase::WRITE_FILE, "write"},
      {Phase::READ_FILE, "read"},
      {Phase::LINK, "link"},
      {Phase::CACHE_ADD, "cache_add"},
      {Phase::ENQUEUE_UPLOAD, "enqueue"},
  };

  auto it = phases.find(phase);
  if (it != phases.end()) {
    return it->second;
  }
  return "unknown";
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

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs
