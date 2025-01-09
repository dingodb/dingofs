/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * @Project: dingo
 * @Date: 2021-06-23 21:05:29
 * @Author: chenwei
 */

#ifndef DINGOFS_SRC_COMMON_DEFINE_H_
#define DINGOFS_SRC_COMMON_DEFINE_H_
#include <cstdint>

namespace dingofs {
const uint64_t ROOTINODEID = 1;
const uint64_t RECYCLEINODEID = 2;
const char RECYCLENAME[] = ".recycle";
const uint64_t STATSINODEID = 0x7FFFFFFF00000001;
const char STATSNAME[] = ".stats";

inline bool IsInternalNode(uint64_t ino) {
  return ino == STATSINODEID || ino == RECYCLEINODEID || ino == ROOTINODEID;
}

inline bool IsInternalName(const std::string& name) {
  return name == STATSNAME || name == RECYCLENAME;
}

}  // namespace dingofs
#endif  // DINGOFS_SRC_COMMON_DEFINE_H_
