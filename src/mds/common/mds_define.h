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
 * Created Date: 2021-08-24
 * Author: wanghai01
 */

#ifndef DINGOFS_SRC_MDS_COMMON_MDS_DEFINE_H_
#define DINGOFS_SRC_MDS_COMMON_MDS_DEFINE_H_

#include <cstdint>
namespace dingofs {
namespace mds {

namespace topology {

using FsIdType = uint32_t;
using PoolIdType = uint32_t;
using ZoneIdType = uint32_t;
using ServerIdType = uint32_t;
using MetaServerIdType = uint32_t;
using PartitionIdType = uint32_t;
using CopySetIdType = uint32_t;
using EpochType = uint64_t;
using UserIdType = uint32_t;
using MemcacheClusterIdType = uint32_t;

const uint32_t UNINITIALIZE_ID = 0u;
const uint32_t UNINITIALIZE_COUNT = UINT32_MAX;

}  // namespace topology
}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_SRC_MDS_COMMON_MDS_DEFINE_H_
