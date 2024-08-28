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
 * Created Date: 2024-08-23
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_BASE_FILE_FILE_H_
#define CURVEFS_SRC_BASE_FILE_FILE_H_

#include <sys/stat.h>

#include <string>

namespace curvefs {
namespace base {
namespace file {

inline bool IsFile(const struct stat* stat) { return S_ISREG(stat->st_mode); }

inline bool IsDir(const struct stat* stat) { return S_ISDIR(stat->st_mode); }

inline bool IsLink(const struct stat* stat) { return S_ISLNK(stat->st_mode); }

std::string StrMode(uint16_t mode);

}  // namespace file
}  // namespace base
}  // namespace curvefs

#endif  // CURVEFS_SRC_BASE_FILE_FILE_H_
