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

#include "curvefs/src/base/filepath/filepath.h"

#include <algorithm>

#include "absl/strings/match.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"

namespace curvefs {
namespace base {
namespace filepath {

std::string ParentDir(const std::string& path) {
  size_t index = path.find_last_of("/");
  if (index == std::string::npos) {
    return "/";
  }
  std::string parent = path.substr(0, index);
  if (parent.size() == 0) {
    return "/";
  }
  return parent;
}

std::string Filename(const std::string& path) {
  size_t index = path.find_last_of("/");
  if (index == std::string::npos) {
    return path;
  }
  return path.substr(index + 1, path.length());
}

bool HasSuffix(const std::string& path, const std::string& suffix) {
  return ::absl::EndsWith(path, suffix);
}

std::vector<std::string> PathSplit(const std::string& path) {
  std::vector<std::string> out;
  std::vector<std::string> names = ::absl::StrSplit(path, '/');
  std::copy_if(names.begin(), names.end(), std::back_inserter(out),
               [](const std::string& name) { return name.length() > 0; });
  return out;
}

std::string PathJoin(const std::vector<std::string>& subpaths) {
  return ::absl::StrJoin(subpaths, "/");
}

}  // namespace filepath
}  // namespace base
}  // namespace curvefs
