// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "curvefs/src/mdsv2/common/helper.h"

#include <filesystem>

#include "butil/strings/string_split.h"
#include "curvefs/src/mdsv2/common/logging.h"

namespace dingofs {

namespace mdsv2 {

int64_t Helper::TimestampNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
      .count();
}

int64_t Helper::TimestampUs() {
  return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
      .count();
}

int64_t Helper::TimestampMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
      .count();
}

int64_t Helper::Timestamp() {
  return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

std::string Helper::NowTime() { return FormatMsTime(TimestampMs(), "%Y-%m-%d %H:%M:%S"); }

std::string Helper::FormatMsTime(int64_t timestamp, const std::string& format) {
  std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> tp(
      (std::chrono::milliseconds(timestamp)));

  auto in_time_t = std::chrono::system_clock::to_time_t(tp);
  std::stringstream ss;
  ss << std::put_time(std::localtime(&in_time_t), format.c_str()) << "." << timestamp % 1000;
  return ss.str();
}

std::string Helper::FormatMsTime(int64_t timestamp) { return FormatMsTime(timestamp, "%Y-%m-%d %H:%M:%S"); }

std::string Helper::FormatTime(int64_t timestamp, const std::string& format) {
  std::chrono::time_point<std::chrono::system_clock, std::chrono::seconds> tp((std::chrono::seconds(timestamp)));

  auto in_time_t = std::chrono::system_clock::to_time_t(tp);
  std::stringstream ss;
  ss << std::put_time(std::localtime(&in_time_t), format.c_str());
  return ss.str();
}

std::string Helper::GetNowFormatMsTime() {
  int64_t timestamp = TimestampMs();
  std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> tp(
      (std::chrono::milliseconds(timestamp)));

  auto in_time_t = std::chrono::system_clock::to_time_t(tp);
  std::stringstream ss;
  ss << std::put_time(std::localtime(&in_time_t), "%Y-%m-%dT%H:%M:%S.000Z");
  return ss.str();
}

bool Helper::IsEqualIgnoreCase(const std::string& str1, const std::string& str2) {
  if (str1.size() != str2.size()) {
    return false;
  }
  return std::equal(str1.begin(), str1.end(), str2.begin(),
                    [](const char c1, const char c2) { return std::tolower(c1) == std::tolower(c2); });
}

bool Helper::IsExistPath(const std::string& path) { return std::filesystem::exists(path); }

bool Helper::StringToBool(const std::string& str) { return !(str == "0" || str == "false"); }
int32_t Helper::StringToInt32(const std::string& str) { return std::strtol(str.c_str(), nullptr, 10); }
int64_t Helper::StringToInt64(const std::string& str) { return std::strtoll(str.c_str(), nullptr, 10); }
float Helper::StringToFloat(const std::string& str) { return std::strtof(str.c_str(), nullptr); }
double Helper::StringToDouble(const std::string& str) { return std::strtod(str.c_str(), nullptr); }

void Helper::SplitString(const std::string& str, char c, std::vector<std::string>& vec) {
  butil::SplitString(str, c, &vec);
}

void Helper::SplitString(const std::string& str, char c, std::vector<int64_t>& vec) {
  std::vector<std::string> strs;
  SplitString(str, c, strs);
  for (auto& s : strs) {
    try {
      vec.push_back(std::stoll(s));
    } catch (const std::exception& e) {
      DINGO_LOG(ERROR) << "stoll exception: " << e.what();
    }
  }
}

std::string Helper::StringToHex(const std::string& str) {
  std::stringstream ss;
  for (const auto& ch : str) {
    ss << std::setw(2) << std::setfill('0') << std::hex << static_cast<int>(static_cast<unsigned char>(ch));
  }
  return ss.str();
}

std::string Helper::StringToHex(const std::string_view& str) {
  std::stringstream ss;
  for (const auto& ch : str) {
    ss << std::setw(2) << std::setfill('0') << std::hex << static_cast<int>(static_cast<unsigned char>(ch));
  }
  return ss.str();
}

std::string Helper::HexToString(const std::string& hex_str) {
  std::string result;

  try {
    // The hex_string must be of even length
    for (size_t i = 0; i < hex_str.length(); i += 2) {
      std::string hex_byte = hex_str.substr(i, 2);
      // Convert the hex byte to an integer
      int byte_value = std::stoi(hex_byte, nullptr, 16);
      // Cast the integer to a char and append it to the result string
      result += static_cast<unsigned char>(byte_value);
    }
  } catch (const std::invalid_argument& ia) {
    DINGO_LOG(ERROR) << "HexToString error Irnvalid argument: " << ia.what() << '\n';
    return "";
  } catch (const std::out_of_range& oor) {
    DINGO_LOG(ERROR) << "HexToString error Out of Range error: " << oor.what() << '\n';
    return "";
  }

  return result;
}

}  // namespace mdsv2
}  // namespace dingofs