/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

#include "cache/tools/bench/common/format.h"

#include <algorithm>
#include <cstddef>
#include <iomanip>
#include <sstream>

namespace dingofs {
namespace cache {
namespace bench {

std::string FormatBytes(uint64_t bytes) {
  const char* units[] = {"B", "KiB", "MiB", "GiB", "TiB"};
  double value = static_cast<double>(bytes);
  size_t unit = 0;
  while (value >= 1024.0 && unit + 1 < sizeof(units) / sizeof(units[0])) {
    value /= 1024.0;
    ++unit;
  }
  std::ostringstream os;
  if (unit == 0) {
    os << bytes << ' ' << units[unit];
  } else {
    os << std::fixed << std::setprecision(2) << value << ' ' << units[unit];
  }
  return os.str();
}

std::string FormatRate(double bytes_per_s) {
  return FormatBytes(static_cast<uint64_t>(std::max(0.0, bytes_per_s))) + "/s";
}

std::string FormatCount(double value) {
  std::ostringstream os;
  os << std::fixed << std::setprecision(2);
  if (value >= 1e6) {
    os << value / 1e6 << " M";
  } else if (value >= 1e3) {
    os << value / 1e3 << " k";
  } else {
    os << std::setprecision(1) << value << " ";
  }
  return os.str();
}

std::string FormatLatencyUs(uint64_t latency_us) {
  std::ostringstream os;
  if (latency_us < 1000) {
    os << latency_us << " us";
  } else if (latency_us < 1000 * 1000) {
    os << std::fixed << std::setprecision(2) << latency_us / 1000.0 << " ms";
  } else {
    os << std::fixed << std::setprecision(3) << latency_us / 1000000.0 << " s";
  }
  return os.str();
}

std::string FormatNanos(uint64_t ns) {
  std::ostringstream os;
  if (ns < 1000) {
    os << ns << " ns";
  } else if (ns < 1000ULL * 1000) {
    os << std::fixed << std::setprecision(2) << ns / 1000.0 << " us";
  } else if (ns < 1000ULL * 1000 * 1000) {
    os << std::fixed << std::setprecision(2) << ns / 1000000.0 << " ms";
  } else {
    os << std::fixed << std::setprecision(2) << ns / 1000000000.0 << " s";
  }
  return os.str();
}

std::string FormatDuration(double seconds) {
  std::ostringstream os;
  if (seconds < 60.0) {
    os << std::fixed << std::setprecision(1) << seconds << "s";
  } else {
    const auto minutes = static_cast<uint64_t>(seconds / 60);
    const double rest = seconds - (minutes * 60);
    os << minutes << "m" << std::fixed << std::setprecision(0) << rest << "s";
  }
  return os.str();
}

}  // namespace bench
}  // namespace cache
}  // namespace dingofs
