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
 * Created Date: 2024-08-20
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_UTILS_TIME_H_
#define DINGOFS_SRC_UTILS_TIME_H_

#include <chrono>
#include <cstdint>
#include <ostream>

namespace dingofs {
namespace utils {

struct TimeSpec {
  TimeSpec() : seconds(0), nanoSeconds(0) {}

  TimeSpec(uint64_t seconds, uint32_t nanoSeconds = 0)
      : seconds(seconds), nanoSeconds(nanoSeconds) {}

  TimeSpec(const TimeSpec& time)
      : seconds(time.seconds), nanoSeconds(time.nanoSeconds) {}

  TimeSpec& operator=(const TimeSpec& time) = default;

  TimeSpec operator+(const TimeSpec& time) const {
    return TimeSpec(seconds + time.seconds, nanoSeconds + time.nanoSeconds);
  }

  uint64_t seconds;
  uint32_t nanoSeconds;
};

inline bool operator==(const TimeSpec& lhs, const TimeSpec& rhs) {
  return (lhs.seconds == rhs.seconds) && (lhs.nanoSeconds == rhs.nanoSeconds);
}

inline bool operator!=(const TimeSpec& lhs, const TimeSpec& rhs) {
  return !(lhs == rhs);
}

inline bool operator<(const TimeSpec& lhs, const TimeSpec& rhs) {
  return (lhs.seconds < rhs.seconds) ||
         (lhs.seconds == rhs.seconds && lhs.nanoSeconds < rhs.nanoSeconds);
}

inline bool operator>(const TimeSpec& lhs, const TimeSpec& rhs) {
  return (lhs.seconds > rhs.seconds) ||
         (lhs.seconds == rhs.seconds && lhs.nanoSeconds > rhs.nanoSeconds);
}

inline std::ostream& operator<<(std::ostream& os, const TimeSpec& time) {
  return os << time.seconds << "." << time.nanoSeconds;
}

inline TimeSpec TimeNow() {
  struct timespec now;
  clock_gettime(CLOCK_REALTIME, &now);
  return TimeSpec(now.tv_sec, now.tv_nsec);
}

inline int64_t TimestampNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

inline int64_t TimestampUs() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

inline int64_t TimestampMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

inline int64_t Timestamp() {
  return std::chrono::duration_cast<std::chrono::seconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

}  // namespace utils
}  // namespace dingofs

#endif  // DINGOFS_SRC_UTILS_TIME_H_
