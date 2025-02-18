/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: dingo
 * Created Date: Mon Aug 30 2021
 * Author: hzwuhongsong
 */

#ifndef DINGOFS_SRC_COMMON_UTILS_H_
#define DINGOFS_SRC_COMMON_UTILS_H_

#include <string>

namespace dingofs {
namespace common {

class SysUtils {
 public:
  SysUtils() {}
  ~SysUtils() {}
  std::string RunSysCmd(const std::string& cmd);
};

}  // namespace common
}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_UTILS_H_
