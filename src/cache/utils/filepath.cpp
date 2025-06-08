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

#include "cache/utils/filepath.h"

#include <absl/strings/match.h>
#include <absl/strings/str_join.h>

namespace dingofs {
namespace cache {

std::string FilePath::ParentDir(const std::string& path) {
  size_t index = path.find_last_of('/');
  if (index == std::string::npos) {
    return "/";
  }

  std::string parent = path.substr(0, index);
  if (parent.empty()) {
    return "/";
  }
  return parent;
}

std::string FilePath::Filename(const std::string& path) {
  size_t index = path.find_last_of('/');
  if (index == std::string::npos) {
    return path;
  }
  return path.substr(index + 1, path.length());
}

bool FilePath::HasSuffix(const std::string& path, const std::string& suffix) {
  return absl::EndsWith(path, suffix);
}

std::string FilePath::PathJoin(const std::vector<std::string>& subpaths) {
  return absl::StrJoin(subpaths, "/");
}

}  // namespace cache
}  // namespace dingofs
