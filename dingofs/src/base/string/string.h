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

#ifndef DINGOFS_SRC_BASE_STRING_STRING_H_
#define DINGOFS_SRC_BASE_STRING_STRING_H_

#include <algorithm>
#include <stdexcept>
#include <string>
#include <vector>

#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "utils/uuid.h"

namespace dingofs {
namespace base {
namespace string {

using ::absl::StrFormat;
using ::absl::StrJoin;
using ::absl::StrSplit;
using ::dingofs::utils::UUIDGenerator;

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

inline std::string GenUuid() { return UUIDGenerator().GenerateUUID(); }

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

}  // namespace string
}  // namespace base
}  // namespace dingofs

#endif  // DINGOFS_SRC_BASE_STRING_STRING_H_
