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

#include "curvefs/src/base/file/file.h"

#include <unordered_map>

namespace curvefs {
namespace base {
namespace file {

std::string StrMode(uint16_t mode) {
  static std::unordered_map<uint16_t, char> type2char = {
    {S_IFSOCK, 's'},
    {S_IFLNK, 'l'},
    {S_IFREG, '-'},
    {S_IFBLK, 'b'},
    {S_IFDIR, 'd'},
    {S_IFCHR, 'c'},
    {S_IFIFO, 'f'},
    {0, '?'},
  };

  std::string s("?rwxrwxrwx");
  s[0] = type2char[mode & (S_IFMT & 0xffff)];
  if (mode & S_ISUID) {
    s[3] = 's';
  }
  if (mode & S_ISGID) {
    s[6] = 's';
  }
  if (mode & S_ISVTX) {
    s[9] = 't';
  }

  for (auto i = 0; i < 9; i++) {
    if ((mode & (1 << i)) == 0) {
      if ((s[9 - i] == 's') || (s[9 - i] == 't')) {
        s[9 - i] &= 0xDF;
      } else {
        s[9 - i] = '-';
      }
    }
  }
  return s;
}

}  // namespace file
}  // namespace base
}  // namespace curvefs
