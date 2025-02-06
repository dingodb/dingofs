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
 * Created Date: Thur Oct 14 2021
 * Author: majie1
 */

#ifndef DINGOFS_SRC_COMMON_S3UTIL_H_
#define DINGOFS_SRC_COMMON_S3UTIL_H_

#include <cstdint>
#include <string>

#include "cache/blockcache/cache_store.h"

namespace dingofs {
namespace common {
namespace s3util {

using ::dingofs::client::blockcache::BlockKey;

inline std::string GenObjName(uint64_t chunkid, uint64_t index,
                              uint64_t compaction, uint64_t fsid,
                              uint64_t inodeid, uint32_t objectPrefix) {
  BlockKey key(fsid, inodeid, chunkid, index, compaction);
  return key.StoreKey();
}

}  // namespace s3util
}  // namespace common
}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_S3UTIL_H_
