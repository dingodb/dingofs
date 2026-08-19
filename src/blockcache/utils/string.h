/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_BLOCKCACHE_UTILS_STRING_H_
#define DINGOFS_BLOCKCACHE_UTILS_STRING_H_

#include <charconv>
#include <string>
#include <string_view>
#include <vector>

namespace dingofs {
namespace blockcache {

template <typename T>
inline bool SplitUint(std::string_view* sv, char sep, T* out) {
  const size_t end = sep == '\0' ? sv->size() : sv->find(sep);
  if (end == std::string_view::npos || end == 0) {
    return false;
  }
  const char* first = sv->data();
  const auto [ptr, ec] = std::from_chars(first, first + end, *out);
  if (ec != std::errc() || ptr != first + end) {
    return false;
  }
  sv->remove_prefix(end == sv->size() ? end : end + 1);
  return true;
}

inline std::vector<std::string> Split(std::string_view value, char sep) {
  std::vector<std::string> items;
  for (size_t begin = 0; begin <= value.size();) {
    size_t end = value.find(sep, begin);
    if (end == std::string_view::npos) {
      end = value.size();
    }
    if (end > begin) {
      items.emplace_back(value.substr(begin, end - begin));
    }
    begin = end + 1;
  }
  return items;
}

inline std::string TrimWhitespace(const std::string& value) {
  const auto begin = value.find_first_not_of(" \t\r\n");
  if (begin == std::string::npos) {
    return "";
  }
  const auto end = value.find_last_not_of(" \t\r\n");
  return value.substr(begin, end - begin + 1);
}

inline std::string RedString(const std::string& str) {
  return "\x1B[31m" + str + "\033[0m";
}

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_UTILS_STRING_H_
