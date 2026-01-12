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
 * Created Date: 2024-08-25
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_UTILS_STRING_H_
#define DINGOFS_SRC_UTILS_STRING_H_

#include <algorithm>
#include <cstring>
#include <iomanip>
#include <stdexcept>
#include <string>
#include <vector>

namespace dingofs {
namespace utils {

inline bool Str2Int(const std::string& str, uint64_t* num) noexcept {
  try {
    *num = std::stoull(str);
    return true;
  } catch (std::invalid_argument& e) {
    return false;
  } catch (std::out_of_range& e) {
    return false;
  }
}

inline bool Str2Int(const std::string& str, uint32_t* num) noexcept {
  try {
    *num = std::stoull(str);
    return true;
  } catch (std::invalid_argument& e) {
    return false;
  } catch (std::out_of_range& e) {
    return false;
  }
}

inline bool Strs2Ints(const std::vector<std::string>& strs,
                      const std::vector<uint64_t*>& nums) {
  if (strs.size() != nums.size()) {
    return false;
  }

  for (size_t i = 0; i < strs.size(); i++) {
    if (!Str2Int(strs[i], nums[i])) {
      return false;
    }
  }
  return true;
}

inline std::string TrimSpace(const std::string& str) {
  std::string s = str;

  s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](unsigned char ch) {
            return !std::isspace(ch);
          }));

  s.erase(std::find_if(s.rbegin(), s.rend(),
                       [](unsigned char ch) { return !std::isspace(ch); })
              .base(),
          s.end());

  return s;
}

// remove whitespace、\r、\n from string
inline std::string TrimAsciiWhitespace(const std::string& str) {
  std::string s = str;

  s.erase(std::remove_if(s.begin(), s.end(),
                         [](unsigned char ch) {
                           return ch == ' ' || ch == '\n' || ch == '\r';
                         }),
          s.end());

  return s;
}

inline std::string TrimChars(const std::string& str,
                             const std::string& chars_to_trim) {
  size_t first = str.find_first_not_of(chars_to_trim);
  if (first == std::string::npos) return "";

  size_t last = str.find_last_not_of(chars_to_trim);

  return str.substr(first, (last - first + 1));
}

// convert hex char to int value
inline int HexCharToInt(char c) {
  if (c >= '0' && c <= '9') {
    return c - '0';
  } else if (c >= 'A' && c <= 'F') {
    return 10 + (c - 'A');
  } else if (c >= 'a' && c <= 'f') {
    return 10 + (c - 'a');
  } else {
    return -1;
  }
}

// convert hex string to byte buffer
inline int HexStringToBuf(const char* hex_str, unsigned char* buf,
                          size_t buf_size) {
  size_t hex_len = strlen(hex_str);
  if (hex_len % 2 != 0) {
    return -1;
  }
  size_t buf_len = hex_len / 2;
  if (buf_len > buf_size) {
    return -1;
  }

  for (size_t i = 0; i < buf_len; i++) {
    int high_nibble = HexCharToInt(hex_str[2 * i]);
    int low_nibble = HexCharToInt(hex_str[2 * i + 1]);
    if (high_nibble == -1 || low_nibble == -1) {
      return -1;
    }

    buf[i] = (high_nibble << 4) | low_nibble;
  }

  return buf_len;
}

// convert buf to hex string
inline std::string BufToHexString(unsigned char* buf, size_t buf_size) {
  std::ostringstream oss;
  for (uint32_t i = 0; i < buf_size; i++) {
    oss << std::hex << std::setw(2) << std::setfill('0') << int((buf)[i]);
  }
  return oss.str();
}

}  // namespace utils
}  // namespace dingofs

#endif  // DINGOFS_SRC_UTILS_STRING_H_
