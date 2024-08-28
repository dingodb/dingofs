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

#ifndef CURVEFS_SRC_BASE_FILEPATH_FILEPATH_H_
#define CURVEFS_SRC_BASE_FILEPATH_FILEPATH_H_

#include <string>
#include <vector>

namespace curvefs {
namespace base {
namespace filepath {

std::string ParentDir(const std::string& path);

std::string Filename(const std::string& path);

bool HasSuffix(const std::string& path, const std::string& suffix);

std::vector<std::string> PathSplit(const std::string& path);

std::string PathJoin(const std::vector<std::string>& subpaths);

}  // namespace filepath
}  // namespace base
}  // namespace curvefs

#endif  // CURVEFS_SRC_BASE_FILEPATH_FILEPATH_H_
