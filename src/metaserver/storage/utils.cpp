/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Project: Dingofs
 * Date: 2022-02-27
 * Author: Jingli Chen (Wine93)
 */

#include "metaserver/storage/utils.h"

#include <glog/logging.h>

#include <algorithm>
#include <cstring>
#include <fstream>

#include "utils/concurrent/concurrent.h"
#include "utils/string_util.h"
#include "fs/ext4_filesystem_impl.h"
#include "fs/fs_common.h"

namespace dingofs {
namespace metaserver {
namespace storage {

using ::dingofs::utils::ReadLockGuard;
using ::dingofs::utils::StringToUll;
using ::dingofs::utils::WriteLockGuard;
using ::dingofs::fs::Ext4FileSystemImpl;

bool GetFileSystemSpaces(const std::string& path, uint64_t* total,
                         uint64_t* available) {
  struct dingofs::fs::FileSystemInfo info;

  auto localFS = Ext4FileSystemImpl::getInstance();
  int ret = localFS->Statfs(path, &info);
  if (ret != 0) {
    LOG(ERROR) << "Failed to get file system space information"
               << ", error message: " << strerror(errno);
    return false;
  }

  *total = info.total;
  *available = info.available;
  return true;
}

bool GetProcMemory(uint64_t* vmRSS) {
  std::string fileName = "/proc/self/status";
  std::ifstream file(fileName);
  if (!file.is_open()) {
    LOG(ERROR) << "Open file " << fileName << " failed";
    return false;
  }

  std::string line;
  while (getline(file, line)) {
    auto position = line.find("VmRSS:");
    if (position == line.npos) {
      continue;
    }

    std::string value = line.substr(position + 6);
    position = value.find("kB");
    value = value.substr(0, position);

    value.erase(std::remove_if(value.begin(), value.end(), isspace),
                value.end());
    if (!StringToUll(value, vmRSS)) {
      return false;
    }
    *vmRSS *= 1024;
    return true;
  }

  return false;
}

}  // namespace storage
}  // namespace metaserver
}  // namespace dingofs
