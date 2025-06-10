// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGOFS_MDSV2_COMMON_CONSTANT_H_
#define DINGOFS_MDSV2_COMMON_CONSTANT_H_

#include <cstdint>

namespace dingofs {
namespace mdsv2 {

const uint32_t kSetAttrMode = 1 << 0;
const uint32_t kSetAttrUid = 1 << 1;
const uint32_t kSetAttrGid = 1 << 2;
const uint32_t kSetAttrLength = 1 << 3;
const uint32_t kSetAttrAtime = 1 << 4;
const uint32_t kSetAttrMtime = 1 << 5;
const uint32_t kSetAttrCtime = 1 << 6;
const uint32_t kSetAttrNlink = 1 << 7;

const int kEmptyDirMinLinkNum = 2;

const uint64_t kRootIno = 1;
const uint64_t kRootParentIno = 0;

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDSV2_COMMON_CONSTANT_H_
