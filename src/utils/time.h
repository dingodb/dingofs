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
#include <iomanip>
#include <ostream>
#include <sstream>

namespace dingofs {
namespace utils {

inline uint64_t TimestampNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

inline uint64_t TimestampUs() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

inline uint64_t TimestampMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

inline uint64_t Timestamp() {
  return std::chrono::duration_cast<std::chrono::seconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

inline std::string FormatMsTime(int64_t timestamp, const std::string& format) {
  std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>
      tp((std::chrono::milliseconds(timestamp)));

  auto in_time_t = std::chrono::system_clock::to_time_t(tp);
  std::stringstream ss;
  ss << std::put_time(std::localtime(&in_time_t), format.c_str()) << "."
     << timestamp % 1000;
  return ss.str();
}

inline std::string FormatMsTime(int64_t timestamp) {
  return FormatMsTime(timestamp, "%Y-%m-%d %H:%M:%S");
}

inline std::string FormatTime(int64_t timestamp, const std::string& format) {
  std::chrono::time_point<std::chrono::system_clock, std::chrono::seconds> tp(
      (std::chrono::seconds(timestamp)));

  auto in_time_t = std::chrono::system_clock::to_time_t(tp);
  std::stringstream ss;
  ss << std::put_time(std::localtime(&in_time_t), format.c_str());
  return ss.str();
}

inline std::string FormatTime(int64_t timestamp) {
  return FormatTime(timestamp, "%Y-%m-%d %H:%M:%S");
}

inline std::string FormatNsTime(int64_t timestamp) {
  int64_t sec = timestamp / 1000000000;
  int64_t ns = timestamp % 1000000000;
  std::string result = FormatTime(sec);

  return result + "." + std::to_string(ns);
}

inline std::string GetNowFormatMsTime() {
  int64_t timestamp = TimestampMs();
  std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>
      tp((std::chrono::milliseconds(timestamp)));

  auto in_time_t = std::chrono::system_clock::to_time_t(tp);
  std::stringstream ss;
  ss << std::put_time(std::localtime(&in_time_t), "%Y-%m-%dT%H:%M:%S.000Z");
  return ss.str();
}

inline std::string NowTime() {
  return FormatMsTime(TimestampMs(), "%Y-%m-%d %H:%M:%S");
}

struct TimeSpec {
  TimeSpec() : seconds(0), nanoSeconds(0) {}
  TimeSpec(uint64_t seconds, uint32_t nanoSeconds = 0)
      : seconds(seconds), nanoSeconds(nanoSeconds) {}

  TimeSpec(const TimeSpec& time) = default;
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
  clock_gettime(CLOCK_REALTIME, &now);  // NOLINT
  return TimeSpec(now.tv_sec, now.tv_nsec);
}

class Duration {
 public:
  Duration() { start_time_ns_ = TimestampNs(); }
  ~Duration() = default;

  int64_t StartNs() const { return start_time_ns_; }
  int64_t StartUs() const { return start_time_ns_ / 1000; }
  int64_t StartMs() const { return start_time_ns_ / 1000000; }

  // Get elapsed time in nanoseconds
  int64_t ElapsedNs() const { return TimestampNs() - start_time_ns_; }
  int64_t ElapsedUs() const { return (TimestampNs() - start_time_ns_) / 1000; }
  int64_t ElapsedMs() const {
    return (TimestampNs() - start_time_ns_) / 1000000;
  }
  int64_t ElapsedS() const {
    return (TimestampNs() - start_time_ns_) / 1000000000;
  }

 private:
  int64_t start_time_ns_ = 0;
};

}  // namespace utils
}  // namespace dingofs

#endif  // DINGOFS_SRC_UTILS_TIME_H_
