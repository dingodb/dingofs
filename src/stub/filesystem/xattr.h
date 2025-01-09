/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Project: Curve
 * Created Date: 2023-07-19
 * Author: Jingli Chen (Wine93)
 */

#include <cstdint>
#include <map>
#include <string>

#ifndef DINGOFS_SRC_STUB_FILESYSTEM_XATTR_H_
#define DINGOFS_SRC_STUB_FILESYSTEM_XATTR_H_

namespace dingofs {
namespace stub {
namespace filesystem {

const uint32_t MAX_XATTR_NAME_LENGTH = 255;
const uint32_t MAX_XATTR_VALUE_LENGTH = 64 * 1024;

const char XATTR_DIR_FILES[] = "dingo.dir.files";
const char XATTR_DIR_SUBDIRS[] = "dingo.dir.subdirs";
const char XATTR_DIR_ENTRIES[] = "dingo.dir.entries";
const char XATTR_DIR_FBYTES[] = "dingo.dir.fbytes";
const char XATTR_DIR_RFILES[] = "dingo.dir.rfiles";
const char XATTR_DIR_RSUBDIRS[] = "dingo.dir.rsubdirs";
const char XATTR_DIR_RENTRIES[] = "dingo.dir.rentries";
const char XATTR_DIR_RFBYTES[] = "dingo.dir.rfbytes";
const char XATTR_DIR_PREFIX[] = "dingo.dir";
const char XATTR_WARMUP_OP[] = "dingofs.warmup.op";

inline bool IsSpecialXAttr(const std::string& key) {
  static std::map<std::string, bool> xattrs{
      {XATTR_DIR_FILES, true},    {XATTR_DIR_SUBDIRS, true},
      {XATTR_DIR_ENTRIES, true},  {XATTR_DIR_FBYTES, true},
      {XATTR_DIR_RFILES, true},   {XATTR_DIR_RSUBDIRS, true},
      {XATTR_DIR_RENTRIES, true}, {XATTR_DIR_RFBYTES, true},
      {XATTR_DIR_PREFIX, true},
  };
  return xattrs.find(key) != xattrs.end();
}

inline bool IsWarmupXAttr(const std::string& key) {
  return key == XATTR_WARMUP_OP;
}

}  // namespace filesystem
}  // namespace stub
}  // namespace dingofs

#endif  // DINGOFS_SRC_STUB_FILESYSTEM_XATTR_H_
