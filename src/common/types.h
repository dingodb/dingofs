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

#ifndef DINGOFS_CLIENT_COMMON_TYPES_H_
#define DINGOFS_CLIENT_COMMON_TYPES_H_

#include <cstdint>
#include <string>

namespace dingofs {

enum class MetaSystemType : uint8_t {
  MDS,
  LOCAL,
  MEMORY,
  UNKNOWN,
};

inline MetaSystemType ParseMetaSystemType(
    const std::string& metasystem_type_str) {
  if (metasystem_type_str == "mds") {
    return MetaSystemType::MDS;
  } else if (metasystem_type_str == "local") {
    return MetaSystemType::LOCAL;
  } else if (metasystem_type_str == "memory") {
    return MetaSystemType::MEMORY;
  } else {
    return MetaSystemType::UNKNOWN;
  }
}

inline std::string MetaSystemTypeToString(
    const MetaSystemType& metasystem_type) {
  switch (metasystem_type) {
    case MetaSystemType::MDS:
      return "mds";
    case MetaSystemType::LOCAL:
      return "local";
    case MetaSystemType::MEMORY:
      return "memory";
    default:
      return "unknown";
  }
}

}  // namespace dingofs

#endif  // DINGOFS_CLIENT_COMMON_TYPES_H_
