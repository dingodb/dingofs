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
 * Project: dingo
 * Created Date: Thur Sept 3 2021
 * Author: lixiaocui
 */

#ifndef DINGOFS_SRC_CLIENT_VFS_LEGACY_COMMON_H_
#define DINGOFS_SRC_CLIENT_VFS_LEGACY_COMMON_H_

#include <cstdint>
#include <string>

namespace dingofs {
namespace client {
namespace common {

enum DiskCacheType { kDisable = 0, kOnlyRead = 1, kReadWrite = 2 };

constexpr int kWarmupOpNum = 4;

enum WarmupOpType {
  kWarmupOpUnknown = 0,
  kWarmupOpAdd = 1,
  kWarmupOpQuery = 2,
};

WarmupOpType GetWarmupOpType(const std::string& op);

enum class WarmupType {
  kWarmupTypeUnknown = 0,
  kWarmupTypeList = 1,
  kWarmupTypeSingle = 2,
};

WarmupType GetWarmupType(const std::string& type);

enum WarmupStorageType {
  kWarmupStorageTypeUnknown = 0,
  kWarmupStorageTypeDisk = 1,
  kWarmupStorageTypeKvClient = 2,
};

WarmupStorageType GetWarmupStorageType(const std::string& type);

enum FileHandle : uint64_t {
  kDefaultValue = 0,
  kKeepCache = 1,
};

enum NlinkChange : int32_t {
  kAddOne = 1,
  kSubOne = -1,
};

// if direction is true means '+', false means '-'
// is direction is true, add second to first
// if direction is false, sub second from first
bool AddUllStringToFirst(std::string* first, uint64_t second, bool direction);
bool AddUllStringToFirst(uint64_t* first, const std::string& second);

}  // namespace common
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_LEGACY_COMMON_H_
