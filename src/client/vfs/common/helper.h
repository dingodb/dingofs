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

#ifndef DINGODB_CLIENT_VFS_COMMON_HELPER_H
#define DINGODB_CLIENT_VFS_COMMON_HELPER_H

#include <absl/strings/str_format.h>
#include <glog/logging.h>
#include <sys/stat.h>

#include <cstdint>
#include <map>
#include <string>

#include "butil/strings/string_split.h"
#include "client/meta/vfs_meta.h"

namespace dingofs {
namespace client {
namespace vfs {

static std::string StrMode(uint16_t mode) {
  static std::map<uint16_t, char> type2char = {
      {S_IFSOCK, 's'}, {S_IFLNK, 'l'}, {S_IFREG, '-'}, {S_IFBLK, 'b'},
      {S_IFDIR, 'd'},  {S_IFCHR, 'c'}, {S_IFIFO, 'f'}, {0, '?'},
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

static std::string StrAttr(Attr* attr) {
  std::string smode;
  return absl::StrFormat(" (%d,[%s:0%06o,%d,%d,%d,%d,%d,%d,%d])", attr->ino,
                         StrMode(attr->mode).c_str(), attr->mode, attr->nlink,
                         attr->uid, attr->gid, attr->atime, attr->mtime,
                         attr->ctime, attr->length);
}

static void ToTimeSpec(uint64_t timestamp_ns, struct timespec* ts) {
  ts->tv_nsec = timestamp_ns % 1000000000;
  ts->tv_sec = timestamp_ns / 1000000000;
}

static uint64_t ToTimestamp(const struct timespec& ts) {
  return ts.tv_sec * 1000000000 + ts.tv_nsec;
}

static uint64_t ToTimestamp(uint64_t tv_sec, uint32_t tv_nsec) {
  return tv_sec * 1000000000 + tv_nsec;
}

static uint64_t CurrentTimestamp() {
  struct timespec ts;
  (void)clock_gettime(CLOCK_REALTIME, &ts);
  return ToTimestamp(ts);
}

static Attr GenerateVirtualInodeAttr(Ino ino) {
  Attr attr;

  attr.ino = ino;
  attr.mode = S_IFREG | 0444;
  attr.nlink = 1;
  attr.length = 0;

  struct timespec now;
  clock_gettime(CLOCK_REALTIME, &now);
  attr.atime = ToTimestamp(now);
  attr.mtime = ToTimestamp(now);
  attr.ctime = ToTimestamp(now);

  attr.type = FileType::kFile;

  return attr;
}

static void SplitString(const std::string& str, char c,
                        std::vector<std::string>& vec) {
  butil::SplitString(str, c, &vec);
}

static void SplitString(const std::string& str, char c,
                        std::vector<int64_t>& vec) {
  std::vector<std::string> strs;
  SplitString(str, c, strs);
  for (auto& s : strs) {
    try {
      vec.push_back(std::stoll(s));
    } catch (const std::exception& e) {
      LOG(ERROR) << "stoll exception: " << e.what();
    }
  }
}

static std::string FormatTime(int64_t timestamp, const std::string& format) {
  std::chrono::time_point<std::chrono::system_clock, std::chrono::seconds> tp((std::chrono::seconds(timestamp)));

  auto in_time_t = std::chrono::system_clock::to_time_t(tp);
  std::stringstream ss;
  ss << std::put_time(std::localtime(&in_time_t), format.c_str());
  return ss.str();
}

static std::string FormatTime(int64_t timestamp) { return FormatTime(timestamp, "%Y-%m-%d %H:%M:%S"); }

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGODB_CLIENT_VFS_COMMON_HELPER_H
